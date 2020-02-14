# AWS Elemental Adapter

A Python adapter for communicating with the streaming server services by AWS (AWS Elemental). It supports creating the needed infrastructure for streaming servers, mainly:
- AWS Elemental MediaLive Channels, Inputs & Outputs
- AWS Elemental MediaPackage Channel & Endpoints
- Cloudfront CDN distribution

It's also capable of deleting idle channels with all attached entities.

The intended use-case is to have a system spin up streaming server infrastructure as it needs it, eg. for user-generated streaming content (aka Twitch.tv-like services).

### Usage

If you intend to use this, you'll likely want to do some adjustments to the configurations of the created resources (it currently does audio-only HLS with a biased configuration for adaptive streaming and available bitrates)
