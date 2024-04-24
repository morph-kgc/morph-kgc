## Morph-KGC Helm Deployment

### Overview
This directory contains Helm charts for deploying Morph-KGC on Kubernetes. 

### Features
**Flexible Deployment Options:** Choose between deploying with a CronJob at specified intervals or as a one-time deployment. 

- To enable deployment with specified intervals, modify the cronJob.enabled parameter to true and set the desired schedule. For example, to deploy every minute:

```
cronJob:
  enabled: true
  schedule: "* * * * *"
```
In this example:
- The first asterisk represents minutes and * means "every minute".
- The second asterisk represents hours and * means "every hour".
- The third asterisk represents days of the month and * means "every day".
- The fourth asterisk represents months and * means "every month".
- The fifth asterisk represents days of the week and * means "every day of the week".

**ConfigMap Integration:** Ensures that the config.ini and mapping.rml.ttl files are treated as ConfigMaps, with names specified in values.yaml.

Users can manually create ConfigMaps using: 

`kubectl create configmap configmap-config --from-file=files/config.ini`


**File Mounting Configuration:** 
The configuration includes automatic mounting of files in the container:

- config.ini is mounted at `/app/config`.
- The mapping file is mounted at `/app/config/files`.

**Note:** Ensure that the config.ini file contains references to the mapping file starting with `config/files/`.



## Installation

To install Morph-KGC, use the following command:

```bash
helm install morph-kgc ./helm
```