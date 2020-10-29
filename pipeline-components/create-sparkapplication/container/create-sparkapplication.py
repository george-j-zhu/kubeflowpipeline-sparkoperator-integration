# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import uuid
from kubernetes import client, config

def get_k8s_api():
    config.load_incluster_config()
    api = client.CustomObjectsApi()
    return api

def create_sparkapplication(k8s_api):
  # it's my custom resource defined as Dict

  driver_pod_name = "pyspark-pi-" + str(uuid.uuid1())

  my_resource = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
      "name": driver_pod_name,
      "namespace": "kubeflow"
    },
    "spec": {
      "type": "Python",
      "pythonVersion": "3",
      "mode": "cluster",
      "image": "gcr.io/spark-operator/spark-py:v2.4.5",
      "imagePullPolicy": "Always",
      "mainApplicationFile": "local:///opt/spark/examples/src/main/python/pi.py",
      "sparkVersion": "2.4.5",
      "restartPolicy": {
        "type": "OnFailure",
        "onFailureRetries": 3,
        "onFailureRetryInterval": 10,
        "onSubmissionFailureRetries": 5,
        "onSubmissionFailureRetryInterval": 20
      },
      "driver": {
        "cores": 1,
        "coreLimit": "1000m",
        "memory": "512m",
        "labels": {
          "version": "2.4.5"
        },
        "serviceAccount": "spark-operatoroperator-sa"
      },
      "executor": {
        "cores": 1,
        "instances": 1,
        "memory": "512m",
        "labels": {
          "version": "2.4.5"
        }
      }
    }
  }

  # create the resource
  k8s_api.create_namespaced_custom_object(
      group="sparkoperator.k8s.io",
      version="v1beta2",
      namespace="kubeflow",
      plural="sparkapplications",
      body=my_resource,
  )
  print("SparkApplication created")
  print("Spark Driver Pod Name: ", driver_pod_name+"-driver")

def main(argv=None):

  logging.getLogger().setLevel(logging.INFO)
  k8s_api = get_k8s_api()
  step_id = create_sparkapplication(k8s_api)

if __name__== "__main__":
  main()
