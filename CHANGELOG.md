## 0.2.2 - 2016-08-08
* [maintenance] Fix validation logic for `path_prefix` and `paths` options [#23](https://github.com/embulk/embulk-input-gcs/pull/23)

## 0.2.1 - 2016-08-04
* [maintenance] Use retry logic when generate GCS client [#21](https://github.com/embulk/embulk-input-gcs/pull/21)

## 0.2.0 - 2016-06-03
* [new feature] Support path option to allow to specify list of target objects directly @sonots thanks! [#17](https://github.com/embulk/embulk-input-gcs/pull/17)

## 0.1.13 - 2016-02-04
* [maintenance] Upgraded embulk to v0.8.2  [#14](https://github.com/embulk/embulk-input-gcs/pull/14)
* [maintenance] Updated Google HTTP Client Library from 1.19.0 to 2.1.21.0 [#15](https://github.com/embulk/embulk-input-gcs/pull/15)
* [maintenance] Updated Google Cloud Storage API Client Library from v1-rev27-1.19.1 to v1-rev59-1.21.0  [#15](https://github.com/embulk/embulk-input-gcs/pull/15)

## 0.1.11 - 2016-01-25
* [maintenance] Added retry logic [#11](https://github.com/embulk/embulk-input-gcs/pull/11)

## 0.1.10 - 2015-11-07

* [maintenance] Fix resume download logics [#10](https://github.com/embulk/embulk-input-gcs/pull/10)
* [maintenance] Throw ConfigException when files listing failed. @muga thanks! [#9](https://github.com/embulk/embulk-input-gcs/pull/9)

## 0.1.9 - 2015-10-30

* [maintenance] Fix GcsAuthentication object initialization for mapreduce executor. @muga thanks!  [#7](https://github.com/embulk/embulk-input-gcs/pull/7)

## 0.1.8 - 2015-10-29

* [maintenance] Added unit tests [#8](https://github.com/embulk/embulk-input-gcs/pull/8)

## 0.1.7 - 2015-10-06

* [new feature] Added new auth method - json_keyfile of GCP(Google Cloud Platform)'s service account [#5](https://github.com/embulk/embulk-input-gcs/pull/5)
* [maintenance] Supported mapreduce-executor [#4](https://github.com/embulk/embulk-input-gcs/pull/4)

## 0.1.6 - 2015-09-05

* [new feature] Added new auth method - pre-defined access token of GCE(Google Compute Engine) [#3](https://github.com/embulk/embulk-input-gcs/pull/3)

## 0.1.5 - 2015-08-19

* [maintenance] Upgraded embulk version to 0.7.0
* [maintenance] Refactored

## 0.1.4 - 2015-06-27

* [maintenance] Keep last last_path when input files is empty. @frsyuki thanks! [#1](https://github.com/embulk/embulk-input-gcs/pull/1)
* [maintenance] Refactored error handling logics.

## 0.1.3 - 2015-03-16

* [maintenance] Changed supported Java version from 8 to 7
