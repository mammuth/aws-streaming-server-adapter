# AWS Elemental Adapter

A Python adapter for communicating with the streaming server services by AWS (AWS Elemental). It supports creating the needed infrastructure for streaming servers, mainly:
- AWS Elemental MediaLive Channels, Inputs & Outputs
- AWS Elemental MediaPackage Channel & Endpoints
- Cloudfront CDN distribution

It's also capable of deleting idle channels with all attached entities.

The intended use-case is to have a system spin up streaming server infrastructure as it needs it, eg. for user-generated streaming content (aka Twitch.tv-like services).
