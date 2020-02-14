"""
Adapter for communicating with AWS Elemental. It supports creating the needed infrastructure for streaming, mainly:
MediaLive Input & Channel (& Outputs), MediaPackage Channel & Endpoints

It also supports deleting of idle channels and attached entities.

The passed slug (which should be the channel-slug of jalu.tv) is used for ID's wherever possible (as md5 hash),
thus it's important that the slug is unique, otherwise the channel creation won't work - maybe improve that in the future.

"""
import hashlib
import logging
import time
from time import sleep

import boto3
from botocore.exceptions import ClientError

from . import AbstractStreamingServerAdapter
from .exceptions import CouldNotCreateChannel
from . import settings

# SETTINGS
ACCESS_KEY = settings.AWS_ACCESS_KEY
SECRET_KEY = settings.AWS_SECRET_KEY
REGION_NAME = settings.AWS_REGION_NAME
MEDIALIVE_INPUT_SECURITY_GROUP = settings.AWS_MEDIALIVE_INPUT_SECURITY_GROUP
MEDIALIVE_ROLE_ARN = settings.AWS_MEDIALIVE_ROLE_ARN

logger = logging.getLogger(__name__)


class AWSStreamingServer(AbstractStreamingServerAdapter):
    
    def __init__(self):
        boto3_session = boto3.Session(
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            # aws_session_token=SESSION_TOKEN,  # ToDo: Could/Should we use this for long-lived sessions?
            region_name=REGION_NAME,
        )
        
        # Service clients
        self.mediapackage = boto3_session.client('mediapackage')
        self.medialive = boto3_session.client('medialive')
        self.ssm = boto3_session.client('ssm')
        self.cloudfront = boto3_session.client('cloudfront')

    @staticmethod
    def _slug_to_id(slug: str) -> str:
        return hashlib.md5(slug.encode()).hexdigest()

    @staticmethod
    def _get_rtmp_server_from_input(input_response: dict) -> str:
        """
        Extracts rtmp URL from media_live input definition (eg. strips away the streamkey at the end of the url).
        We only return one of the RTMP endpoints
        :param input_response: json response of self.medialive.describe_input()
        :return: rtmp_server url
        """
        return input_response.get('Input').get('Destinations')[0].get('Url').rsplit('/', 1)[0]

    def create_channel(self, slug: str, streamkey: str, start: bool=True, debug: bool=False) -> dict:
        """
        Creating a channel consists of creating and linking the following entities:
        - MediaPackage Channel
        - MediaPackage Endpoint (holds the HLS Playlist)
        - MediaLive Input (RTMP endpoint)
        - MediaLive Channel
        """

        if slug in ['', None] or streamkey in ['', None]:
            logger.error(f'Cannot create MediaPackage Channel. Slug or streamkey is not set')  # noqa
            raise CouldNotCreateChannel('The specified slug or streamkey is not set. Cannot create channel')

        logger.debug(f'Creating channel for {slug}...')

        slug_hash = self._slug_to_id(slug)

        #
        # Create MediaPackage Channel
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/mediapackage.html#MediaPackage.Client.create_channel  # noqa
        #
        try:
            mp_channel_response = self.mediapackage.create_channel(
                Id=slug_hash,
                Description=f'Channel for the jalu.tv channel {slug}. Automatically created.',
            )
        except ClientError as e:
            # We assume that the most common error is, that the channel already exists.
            # Sadly there seems to be no unique error code for determining that...
            try:
                mp_channel_response = self.mediapackage.describe_channel(Id=slug_hash)
                logger.warning(f'Tried to create MediaPackage Channel {slug_hash}, but it already existed. Using the existing one.')  # noqa
            except Exception:
                raise CouldNotCreateChannel(
                    f'Unknown Error when creating the MediaPackage Channel. Exception message:\n{e.message}')
        #
        # Create MediaPackage Endpoint
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/mediapackage.html#MediaPackage.Client.create_origin_endpoint  # noqa
        #
        try:
            mp_origin_endpoints_response = self.mediapackage.create_origin_endpoint(
                Id=slug_hash,
                ChannelId=slug_hash,
                Description=f'Channel for the jalu.tv channel {slug}. Automatically created.',
                HlsPackage={
                    'PlaylistType': 'EVENT',
                    'PlaylistWindowSeconds': 10,  # ToDo: Optimize parameters
                    'SegmentDurationSeconds': 1,  # ToDo: Optimize parameters
                },
            )
        except ClientError as e:
            # We assume that the most common error is, that the endpoint already exists.
            try:
                mp_origin_endpoints_response = self.mediapackage.describe_origin_endpoint(Id=slug_hash)
                logger.warning(f'Tried to create MediaPackage Endpoint {slug_hash}, but it already existed. Using the existing one.')  # noqa
            except Exception:
                raise CouldNotCreateChannel(
                    f'Unknown Error when creating the MediaPackage Endpoint. Exception message:\n{e}')
        #
        # Create MediaLive Input
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/medialive.html#MediaLive.Client.create_channel  # noqa
        #
        try:
            ml_input_response = self.medialive.create_input(
                Destinations=[
                    {'StreamName': f'{slug}/{streamkey}'},
                    {'StreamName': f'{slug}/{streamkey}'},
                ],
                InputSecurityGroups=[
                    MEDIALIVE_INPUT_SECURITY_GROUP,
                ],
                Name=slug_hash,
                Type='RTMP_PUSH',
            )
        except ClientError as e:
            # ToDo: Behavior when Input already exists? We don't know that it exists.
            # ToDo: Maybe we simply create a new one. Since we'll have a system which removes detached/idle entities.
            raise CouldNotCreateChannel(f'Unknown Error when creating the MediaLive Input. Exception:\n{e}')

        #
        # Add passwords for the MediaPackage inputs to EC2 parameter store
        #
        try:
            self.ssm.put_parameter(
                Name=f'/medialive/{slug_hash}/A',
                Value=mp_channel_response.get('HlsIngest').get('IngestEndpoints')[0].get('Password'),
                Type='SecureString',
                Overwrite=True,
            )
            self.ssm.put_parameter(
                Name=f'/medialive/{slug_hash}/B',
                Value=mp_channel_response.get('HlsIngest').get('IngestEndpoints')[1].get('Password'),
                Type='SecureString',
                Overwrite=True,
            )
        except Exception as e:
            raise CouldNotCreateChannel(f'Could not create password params on SSM. Error:\n{e}')

        #
        # Create MediaLive Channel
        #
        try:
            ml_channel_response = self.medialive.create_channel(
                Destinations=[{
                    'Id': slug_hash,
                    'Settings': [
                        {
                            'PasswordParam': f'/medialive/{slug_hash}/A',
                            'Url': mp_channel_response.get('HlsIngest').get('IngestEndpoints')[0].get('Url'),
                            'Username': mp_channel_response.get('HlsIngest').get('IngestEndpoints')[0].get('Username'),
                        },
                        {
                            'PasswordParam': f'/medialive/{slug_hash}/B',
                            'Url': mp_channel_response.get('HlsIngest').get('IngestEndpoints')[1].get('Url'),
                            'Username': mp_channel_response.get('HlsIngest').get('IngestEndpoints')[1].get('Username'),
                        },
                    ]
                }],
                EncoderSettings={
                    'AudioDescriptions': [{
                        'AudioSelectorName': slug_hash,  # AudioSelector's name used as the source for this AudioDescription
                        'AudioTypeControl': 'FOLLOW_INPUT',
                        'LanguageCodeControl': 'FOLLOW_INPUT',
                        'Name': slug_hash  # Outputs will use this name to uniquely identify this AudioDescription
                    }],
                    'OutputGroups': [{
                        'Name': slug_hash,
                        'OutputGroupSettings': {
                            'HlsGroupSettings': {
                                'ClientCache': 'DISABLED',  # When set to 'disabled', sets the #EXT-X-ALLOW-CACHE:no tag in the manifest, which prevents clients from saving media segments for later replay  # noqa
                                'Destination': {
                                    'DestinationRefId': slug_hash
                                },
                                'HlsCdnSettings': {
                                    'HlsBasicPutSettings': {
                                        'ConnectionRetryInterval': 1,
                                        'FilecacheDuration': 0,
                                        'NumRetries': 10,
                                        'RestartDelay': 15
                                    },
                                },
                                'IndexNSegments': 3,
                                # 'InputLossAction': 'PAUSE_OUTPUT',
                                'KeepSegments': 3,
                                'ManifestDurationFormat': 'FLOATING_POINT',  # There was some reason why integer didn't work in the GUI. Not sure about it anymore  # noqa
                                'Mode': 'LIVE',
                                'SegmentLength': 1,
                                # 'SegmentationMode': 'USE_INPUT_SEGMENTATION' | 'USE_SEGMENT_DURATION',  # ToDo Might be interesting?  # noqa
                            },
                        },
                        'Outputs': [{
                                'AudioDescriptionNames': [slug_hash],
                                'OutputSettings': {
                                    'HlsOutputSettings': {
                                        'HlsSettings': {
                                            'AudioOnlyHlsSettings': {
                                                'AudioTrackType': 'AUDIO_ONLY_VARIANT_STREAM'
                                            },
                                            # 'StandardHlsSettings': {
                                            #     'AudioRenditionSets': 'string',
                                            #     'M3u8Settings': {
                                            #         'AudioFramesPerPes': 123,
                                            #         'AudioPids': 'string',
                                            #         'EcmPid': 'string',
                                            #         'PatInterval': 123,
                                            #         'PcrControl': 'CONFIGURED_PCR_PERIOD'|'PCR_EVERY_PES_PACKET',
                                            #         'PcrPeriod': 123,
                                            #         'PcrPid': 'string',
                                            #         'PmtInterval': 123,
                                            #         'PmtPid': 'string',
                                            #         'ProgramNum': 123,
                                            #         'Scte35Behavior': 'NO_PASSTHROUGH'|'PASSTHROUGH',
                                            #         'Scte35Pid': 'string',
                                            #         'TimedMetadataBehavior': 'NO_PASSTHROUGH'|'PASSTHROUGH',
                                            #         'TimedMetadataPid': 'string',
                                            #         'TransportStreamId': 123,
                                            #         'VideoPid': 'string'
                                            #     }
                                            # }
                                        },
                                        'NameModifier': 'string',
                                        'SegmentModifier': 'string'
                                    },
                                },
                            }],
                    }],
                    'TimecodeConfig': {
                        'Source': 'EMBEDDED'
                    },
                    'VideoDescriptions': []
                },
                InputAttachments=[{
                    'InputAttachmentName': slug_hash,
                    'InputId': ml_input_response.get('Input').get('Id'),
                    # 'InputSettings': {}  # ToDo?
                }],
                # ToDo: This specifies pricing! I think it also prevents the stream to funcion in case the RTMP input has higher bitrate / different input codec?  # noqa
                InputSpecification={
                    'Codec': 'MPEG2',
                    'MaximumBitrate': 'MAX_10_MBPS',
                    'Resolution': 'SD'
                },
                LogLevel='INFO' if debug is True else 'WARNING',
                Name=slug_hash,
                RoleArn=MEDIALIVE_ROLE_ARN
            )
        except ClientError as e:
            raise CouldNotCreateChannel(f'Error creating media live channel for user {slug}. Error:\n{e}')

        #
        # Create CloudFront Distribution
        #
        mp_origin_endpoint_url = mp_origin_endpoints_response.get('Url')
        mp_origin_endpoint_url_parts = mp_origin_endpoint_url.lstrip('https://').split('/')
        mp_origin_endpoint_domain = mp_origin_endpoint_url_parts[0]
        cloudflare_origin_id = 'EMP-' + mp_origin_endpoint_domain.split('.')[0]
        cloudfront_response = self.cloudfront.create_distribution(
            DistributionConfig={
                'Enabled': True,
                'PriceClass': 'PriceClass_100',  # ToDo: Evaluate change to PriceClass_All
                'Comment': f'Automatically created by jalu.tv for channel {slug}',
                'CallerReference': str(time.time()),
                'Origins': {
                    'Quantity': 2,
                    'Items': [
                        # Origin for the MediaPackage Origin Endpoint
                        {
                            'Id': cloudflare_origin_id,
                            'DomainName': mp_origin_endpoint_domain,
                            'OriginPath': '',
                            'CustomHeaders': {
                                'Quantity': 0,
                            },
                            'CustomOriginConfig': {
                                'HTTPPort': 80,
                                'HTTPSPort': 443,
                                'OriginProtocolPolicy': 'match-viewer',
                                'OriginSslProtocols': {
                                    'Quantity': 3,
                                    'Items': [
                                        'TLSv1',
                                        'TLSv1.1',
                                        'TLSv1.2',
                                    ]
                                },
                                'OriginReadTimeout': 30,
                                'OriginKeepaliveTimeout': 5
                            }
                        },
                        {  # Generic MediaPackage origin
                            'Id': 'TEMP_ORIGIN_ID/channel',
                            'DomainName': 'mediapackage.amazonaws.com',
                            'OriginPath': '',
                            'CustomHeaders': {
                                'Quantity': 0,
                            },
                            'CustomOriginConfig': {
                                'HTTPPort': 80,
                                'HTTPSPort': 443,
                                'OriginProtocolPolicy': 'match-viewer',
                                'OriginSslProtocols': {
                                    'Quantity': 3,
                                    'Items': [
                                        'TLSv1',
                                        'TLSv1.1',
                                        'TLSv1.2',
                                    ]
                                },
                                'OriginReadTimeout': 30,
                                'OriginKeepaliveTimeout': 5
                            }
                        },
                    ]
                },
                'CacheBehaviors': {
                    'Items': [
                        {
                            'TrustedSigners': {
                                'Enabled': False,
                                'Quantity': 0
                            },
                            'LambdaFunctionAssociations': {
                                'Quantity': 0
                            },
                            'TargetOriginId': cloudflare_origin_id,
                            'ViewerProtocolPolicy': 'redirect-to-https',
                            'ForwardedValues': {
                                'Headers': {
                                    'Quantity': 0
                                },
                                'Cookies': {
                                    'Forward': 'none'
                                },
                                'QueryStringCacheKeys': {
                                    'Items': [
                                        'end',
                                        'm',
                                        'start'
                                    ],
                                    'Quantity': 3
                                },
                                'QueryString': True
                            },
                            'MaxTTL': 31536000,
                            'PathPattern': 'index.ism/*',
                            'SmoothStreaming': True,
                            'DefaultTTL': 86400,
                            'AllowedMethods': {
                                'Items': [
                                    'HEAD',
                                    'GET'
                                ],
                                'CachedMethods': {
                                    'Items': [
                                        'HEAD',
                                        'GET'
                                    ],
                                    'Quantity': 2
                                },
                                'Quantity': 2
                            },
                            'MinTTL': 0,
                            'Compress': False
                        },
                        {
                            'TrustedSigners': {
                                'Enabled': False,
                                'Quantity': 0
                            },
                            'LambdaFunctionAssociations': {
                                'Quantity': 0
                            },
                            # 'TargetOriginId': 'TEMP_ORIGIN_ID/channel',
                            'TargetOriginId': cloudflare_origin_id,
                            'ViewerProtocolPolicy': 'redirect-to-https',
                            'ForwardedValues': {
                                'Headers': {
                                    'Quantity': 0
                                },
                                'Cookies': {
                                    'Forward': 'none'
                                },
                                'QueryStringCacheKeys': {
                                    'Items': [
                                        'end',
                                        'm',
                                        'start'
                                    ],
                                    'Quantity': 3
                                },
                                'QueryString': True
                            },
                            'MaxTTL': 31536000,
                            'PathPattern': '*',
                            'SmoothStreaming': False,
                            'DefaultTTL': 86400,
                            'AllowedMethods': {
                                'Items': [
                                    'HEAD',
                                    'GET'
                                ],
                                'CachedMethods': {
                                    'Items': [
                                        'HEAD',
                                        'GET'
                                    ],
                                    'Quantity': 2
                                },
                                'Quantity': 2
                            },
                            'MinTTL': 0,
                            'Compress': False
                        }
                    ],
                    'Quantity': 2
                },
                'DefaultCacheBehavior': {
                    'TrustedSigners': {
                        'Enabled': False,
                        'Quantity': 0
                    },
                    'LambdaFunctionAssociations': {
                        'Quantity': 0
                    },
                    'TargetOriginId': 'TEMP_ORIGIN_ID/channel',
                    'ViewerProtocolPolicy': 'redirect-to-https',
                    'ForwardedValues': {
                        'Headers': {
                            'Quantity': 0
                        },
                        'Cookies': {
                            'Forward': 'none'
                        },
                        'QueryStringCacheKeys': {
                            'Items': [
                                'end',
                                'm',
                                'start'
                            ],
                            'Quantity': 3
                        },
                        'QueryString': True
                    },
                    'MaxTTL': 31536000,
                    'SmoothStreaming': False,
                    'DefaultTTL': 86400,
                    'AllowedMethods': {
                        'Items': [
                            'HEAD',
                            'GET'
                        ],
                        'CachedMethods': {
                            'Items': [
                                'HEAD',
                                'GET'
                            ],
                            'Quantity': 2
                        },
                        'Quantity': 2
                    },
                    'MinTTL': 0,
                    'Compress': False
                },
            }
        )
        cdn_domain = cloudfront_response.get('Distribution').get('DomainName')
        mp_origin_endpoint_path = '/'.join(mp_origin_endpoint_url_parts[1:])
        hls_playlist_cdn_url = f'https://{cdn_domain}/{mp_origin_endpoint_path}'


        #
        # Start MediaLive Channel
        #
        # Channel is still in creating state, we need to wait until it's created in order to start it.
        if start:
            sleep(20)  # ToDo: Repeat channel starting in case it wasn't created yet.
            try:
                ml_start_response = self.medialive.start_channel(ChannelId=ml_channel_response.get('Channel').get('Id'))
            except Exception as e:
                raise CouldNotCreateChannel(
                    f'Could not start the channel with id {mp_channel_response.get("Id")} for {slug}. Error:\n{e}'
                )

        return {
            'hls_playlist_url': mp_origin_endpoints_response.get('Url'),
            'hls_playlist_cdn_url': hls_playlist_cdn_url,
            'cloudflare_distribution_id': cloudfront_response.get('Distribution').get('Id'),
            'rtmp_url': self._get_rtmp_server_from_input(ml_input_response),
            'media_live_channel_id': ml_channel_response.get('Channel').get('Id'),
        }

    def stop_channel(self, channel_id: str) -> None:
        logger.info('Stopping MediaLive channel with ID {channel_id} ')
        self.medialive.stop_channel(ChannelId=channel_id)
        # Wait until stopping is done and then delete idle channels
        # sleep(15)
        # self.delete_idle_channels()

    def delete_idle_channels(self) -> None:
        """
        Deletes the following entities:
        - Idle MediaLive Channels
        - Detached MediaLive Inputs
        - MediaPackage Endpoints referenced by the MediaLive Inputs
        - MediaPackage Channels referenced by the MediaLive Inputs
        - Password parameter store parameters ('/medialive/
        ToDo:
        - Add pagination to deletion if we hit >= 1000 concurrent streams
        - Delete outdated Cloudflare Distributions
        """

        def run_within_try_except(fun):
            try:
                fun()
            except Exception as e:
                pass
                # ToDo: Add logger again once it's not failing every time
                # logger.error(f'Could not delete all channel resources. Error:\n{e}')

        logger.info(f'Deleting idle channels ...')
        ml_channels_response = self.medialive.list_channels(MaxResults=1_000)
        mp_detached_channel_ids = []

        # MediaLive Channels & Inputs
        for channel in ml_channels_response.get('Channels'):
            if channel.get('State') == 'IDLE':
                # Delete Channel
                run_within_try_except(lambda: self.medialive.delete_channel(ChannelId=channel.get('Id')))
                # Store detached ml inputs and mp channel ids
                mp_detached_channel_ids = [destination.get('Id') for destination in channel.get('Destinations')]

        # Delete attached ML inputs
        # It takes a while until each channel is deleted.
        # Before that, their inputs will be 'busy' which prevents deleting
        sleep(5)
        for input in self.medialive.list_inputs(MaxResults=1_000).get('Inputs'):
            if input.get('State') == 'DETACHED':
                run_within_try_except(lambda: self.medialive.delete_input(InputId=input.get('Id')))

        # MediaPackage HLS Endpoints & Channels
        # Their response is paginated by 50
        # Issue with this implementation: If we somehow fail to correctly determine `mp_detached_channel_ids`, a missing
        # channel might be configured forever... I'm not sure how to avoid it, because it looks like we have no clue whether
        # a MP channel is idle or in use.
        mp_channels_response = None
        mp_channels_first_page= True
        while mp_channels_first_page or 'NextToken' in mp_channels_response:
            if mp_channels_first_page:
                mp_channels_response = self.mediapackage.list_channels(MaxResults=50)
                mp_channels_first_page = False
            else:
                mp_channels_response = self.mediapackage.list_channels(
                    MaxResults=50,
                    NextToken=mp_channels_response.get('NextToken')
                )
            for channel in mp_channels_response.get('Channels'):
                if channel.get('Id') in mp_detached_channel_ids:
                    # Delete MediaPackage Endpoints  (we assume it has the channel id (= jalu channe slug) as id)
                    run_within_try_except(lambda: self.mediapackage.delete_origin_endpoint(Id=channel.get('Id')))
                    # Delete MP Channel
                    run_within_try_except(lambda: self.mediapackage.delete_channel(Id=channel.get('Id')))

        for channel in mp_channels_response.get('Channels'):
            if channel.get('Id') in mp_detached_channel_ids:
                for endpoint in channel.get('HlsIngest').get('IngestEndpoints'):
                    # Delete MediaPackage Endpoints
                    run_within_try_except(lambda: self.mediapackage.delete_origin_endpoint(Id=endpoint.get('Id')))
                # Delete MP Channel
                run_within_try_except(lambda: self.mediapackage.delete_channel(Id=channel.get('Id')))

                # SSM Parameter Storage of Endpoint passwords
                run_within_try_except(lambda: self.ssm.delete_parameter(Name=f'/medialive/{channel.get("Id")}/A'))
                run_within_try_except(lambda: self.ssm.delete_parameter(Name=f'/medialive/{channel.get("Id")}/B'))

    def stop_long_running_channels(self) -> None:
        """
        Checks the DB which channels are running longer than allowed (eg. 5h) and calls stop_channel on them.
        """
        from django.db.models import Q
        from api.stream_channels.models import Stream
        from django.utils import timezone
        from datetime import timedelta

        logger.info('Stopping streams which ran too long...')

        streams_over_max_duration = Stream.objects.currently_live().filter(
            start_time__lte=timezone.now() - timedelta(hours=settings.STREAM_MAX_DURATION)
        ).select_related('stream_channel')

        for stream in streams_over_max_duration:
            stream.stream_channel.stop_stream()

    def stop_unused_channels(self) -> None:
        # ToDo Optimize according to the description below:
        """
        In case the user simply never stops streaming, it's elemental resources will run forever,
        producing huge costs.
        This method tracks whether a running channel is used (=incoming traffic?) and stops it in case there was no incoming
        traffic for a certain period of time

        Performed tasks:
        - Query all running channels
        - Store those without incoming traffic including a timestamp
        - Unflag flagged channels in case of incoming traffic
        - Stop channels which have been flagged for a certain amount of time
        - Mark the stream as stopped  # ToDo: Introduce a ending-reason field?
        """
        pass

    def remove_cloudfront_distribution(self, distribution_id: str=None, disable_request_etag: str=None) -> None:
        from api.streaming_server_adapter.tasks import remove_cloudfront_distribution_task
        """
        Removes a CloudFront distribution.

        Two step process: First disable it, then delete it. Since disabling takes some time (>10m?)
        the whole process can take some while.

        For the second step this function is called with the etag of the request which disabled the distribution.
        """
        try:
            if disable_request_etag is None:
                distribution_resp = self.cloudfront.get_distribution(Id=distribution_id)
                if distribution_resp['Distribution']['Status'] != 'Deployed':
                    # If the distribution is currently doing "things" (eg. when it's still starting),
                    # schedule the deletion for later
                    remove_cloudfront_distribution_task.apply_async(
                        (distribution_id, None),
                        countdown=60 * 30  # Wait 30m before running this task because disabling takes some time
                    )
                distribution_config = distribution_resp['Distribution']['DistributionConfig']
                distribution_config['Enabled'] = False
                disable_resp = self.cloudfront.update_distribution(
                    DistributionConfig=distribution_config,
                    Id=distribution_id,
                    IfMatch=distribution_resp['ETag'],
                )
                etag = disable_resp.get('ETag', None)
                if etag is None:
                    logger.error(f'Could not disable CloudFront distribution {distribution_id} Error:\n{disable_resp}')
                remove_cloudfront_distribution_task.apply_async(
                    (distribution_id, etag),
                    countdown=60*30  # Wait 30m before running this task because disabling takes some time
                )
            else:
                self.cloudfront.delete_distribution(Id=distribution_id, IfMatch=disable_request_etag)
        except Exception as e:
            logger.error(f'Could not delete CloudFront distribution {distribution_id} Error:\n{e}')
