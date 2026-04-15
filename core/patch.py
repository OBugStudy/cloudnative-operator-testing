import copy
from typing import Dict, List, Optional

import yaml


def _apply_patch_to_cr(base_cr: dict, patch: dict) -> dict:
    """Apply a LLM-returned patch dict onto base_cr and return the result.

    Patch format (as returned by LLM):
      set:    {dot.path: value, ...}   – fields to add or overwrite
      delete: [dot.path, ...]          – fields to remove

    Dot-path rules:
      "spec.size"                    → cr["spec"]["size"]
      "spec.containers[0].image"     → cr["spec"]["containers"][0]["image"]
      "spec.containers[*].image"     → apply to ALL elements in containers list
                                       (no-op if the list is empty)
      "spec.foo[0]"                  → cr["spec"]["foo"][0]
    """
    result = copy.deepcopy(base_cr)


    _WILDCARD = object()

    def _parse_parts(dotpath: str) -> list:
        """Split a dot-path into (key, index) tuples.

        index is:
          None      – plain dict key          "spec.size"
          int       – concrete list index     "spec.containers[0].image"
          _WILDCARD – broadcast to all items  "spec.containers[*].image"

        "spec.containers[0].image" → [("spec",None),("containers",0),("image",None)]
        "spec.containers[*].image" → [("spec",None),("containers",_WILDCARD),("image",None)]
        """
        parts = []
        for seg in dotpath.split("."):
            if "[" in seg:
                key, rest = seg.split("[", 1)
                idx_str = rest.rstrip("]").strip()
                if idx_str == "*" or idx_str == "":
                    idx = _WILDCARD
                else:
                    try:
                        idx = int(idx_str)
                    except ValueError:
                        idx = _WILDCARD
                parts.append((key, idx))
            else:
                parts.append((seg, None))
        return parts

    def _navigate(node, key: str, idx):
        """Return the child node after one navigation step (no mutation)."""
        if not isinstance(node, dict) or key not in node:
            raise KeyError(key)
        child = node[key]
        if idx is None:
            return child
        if not isinstance(child, list):
            raise TypeError(f"Expected list at '{key}', got {type(child).__name__}")
        if idx is _WILDCARD:
            return child
        if idx >= len(child):
            raise IndexError(f"Index {idx} out of range for '{key}' (len={len(child)})")
        return child[idx]

    def _get_or_create(node, key: str, idx):
        """Navigate one step, auto-creating dicts when needed (no list creation)."""
        if idx is None:
            if not isinstance(node, dict):
                raise TypeError(f"Expected dict at '{key}', got {type(node).__name__}")
            if key not in node:
                node[key] = {}
            return node[key]
        else:
            if not isinstance(node, dict):
                raise TypeError(f"Expected dict at '{key}', got {type(node).__name__}")
            lst = node.get(key)
            if not isinstance(lst, list):
                raise TypeError(f"Expected list at '{key}'")
            if idx is _WILDCARD:
                return lst
            while len(lst) <= idx:
                lst.append({})
            return lst[idx]


    def _set_scalar(root, parts, value) -> None:
        """Set a value when no wildcard is present."""
        node = root
        for key, idx in parts[:-1]:
            node = _get_or_create(node, key, idx)

        last_key, last_idx = parts[-1]
        if last_idx is None:
            if isinstance(node, dict):
                node[last_key] = value
        else:
            if not isinstance(node, dict):
                return
            lst = node.setdefault(last_key, [])
            while len(lst) <= last_idx:
                lst.append({})
            lst[last_idx] = value

    def _set_path(root, dotpath: str, value) -> None:
        parts = _parse_parts(dotpath)


        wildcard_pos = next(
            (i for i, (_, idx) in enumerate(parts) if idx is _WILDCARD), None
        )

        if wildcard_pos is None:

            _set_scalar(root, parts, value)
            return


        node = root
        for key, idx in parts[:wildcard_pos]:
            try:
                node = _get_or_create(node, key, idx)
            except (KeyError, TypeError, IndexError):
                return

        wc_key, _ = parts[wildcard_pos]
        lst = node.get(wc_key) if isinstance(node, dict) else None
        if not isinstance(lst, list) or len(lst) == 0:

            return

        tail = parts[wildcard_pos + 1 :]
        if not tail:

            for i in range(len(lst)):
                lst[i] = value
        else:

            for item in lst:
                if isinstance(item, dict):
                    _set_scalar(item, tail, value)

    def _del_scalar(root, parts) -> None:
        """Delete a path when no wildcard is present."""
        node = root
        for key, idx in parts[:-1]:
            if not isinstance(node, dict) or key not in node:
                return
            child = node[key]
            if idx is not None:
                if not isinstance(child, list) or idx >= len(child):
                    return
                node = child[idx]
            else:
                node = child

        last_key, last_idx = parts[-1]
        if last_idx is None:
            if isinstance(node, dict):
                node.pop(last_key, None)
        else:
            if isinstance(node, dict) and last_key in node:
                lst = node[last_key]
                if isinstance(lst, list) and last_idx < len(lst):
                    lst.pop(last_idx)

    def _del_path(root, dotpath: str) -> None:
        parts = _parse_parts(dotpath)

        wildcard_pos = next(
            (i for i, (_, idx) in enumerate(parts) if idx is _WILDCARD), None
        )

        if wildcard_pos is None:
            _del_scalar(root, parts)
            return


        node = root
        for key, idx in parts[:wildcard_pos]:
            if not isinstance(node, dict) or key not in node:
                return
            child = node[key]
            if idx is not None:
                if not isinstance(child, list) or idx >= len(child):
                    return
                node = child[idx]
            else:
                node = child

        wc_key, _ = parts[wildcard_pos]
        lst = node.get(wc_key) if isinstance(node, dict) else None
        if not isinstance(lst, list) or len(lst) == 0:
            return

        tail = parts[wildcard_pos + 1 :]
        if not tail:

            lst.clear()
        else:
            for item in lst:
                if isinstance(item, dict):
                    _del_scalar(item, tail)


    for dotpath, value in (patch.get("set") or {}).items():
        _set_path(result, dotpath, value)

    for dotpath in patch.get("delete") or []:
        _del_path(result, dotpath)

    return result


def _delete_field_from_cr(cr: dict, field_path: str) -> dict:
    """Return a deep copy of cr with the leaf key at field_path removed."""
    result = copy.deepcopy(cr)
    parts = field_path.split(".")
    obj = result
    for part in parts[:-1]:
        if obj is None:
            return result
        if part.endswith("[*]"):
            key = part[:-3]
            if not isinstance(obj, dict) or key not in obj:
                return result
            obj = obj[key]
            if not isinstance(obj, list) or not obj:
                return result
            obj = obj[0]
        else:
            if not isinstance(obj, dict) or part not in obj:
                return result
            obj = obj[part]
    leaf = parts[-1]
    if leaf.endswith("[*]"):
        leaf = leaf[:-3]
    if isinstance(obj, dict) and leaf in obj:
        del obj[leaf]
    return result


def _cr_field_diff(before_yaml: Optional[str], after_yaml: Optional[str]) -> List[Dict]:
    """对比两个 CR YAML，返回字段级差异列表。

    Returns:
        [{"path": str, "before": any, "after": any}, ...]
    """

    def _flatten(d, prefix=""):
        items = {}
        if isinstance(d, dict):
            for k, v in d.items():
                items.update(_flatten(v, f"{prefix}.{k}" if prefix else k))
        elif isinstance(d, list):
            for i, v in enumerate(d):
                items.update(_flatten(v, f"{prefix}[{i}]"))
        else:
            items[prefix] = d
        return items

    try:
        before_dict = yaml.safe_load(before_yaml or "") or {}
        after_dict = yaml.safe_load(after_yaml or "") or {}
    except Exception:
        return []

    before_flat = _flatten(before_dict)
    after_flat = _flatten(after_dict)
    all_keys = sorted(set(before_flat) | set(after_flat))
    diffs = []
    for k in all_keys:
        bv = before_flat.get(k, "__MISSING__")
        av = after_flat.get(k, "__MISSING__")
        if bv != av:
            diffs.append(
                {
                    "path": k,
                    "before": None if bv == "__MISSING__" else bv,
                    "after": None if av == "__MISSING__" else av,
                }
            )
    return diffs


def _parse_llm_patch(llm_result: str) -> tuple:
    """Parse LLM response that should be a patch YAML.

    Returns (patch_dict, error_str).  patch_dict has keys 'set' and 'delete'.
    On failure returns (None, error_message).
    """
    try:
        parsed = yaml.safe_load(llm_result)
        if not isinstance(parsed, dict):
            return None, f"Expected a YAML mapping, got {type(parsed).__name__}"
        patch = {
            "set": parsed.get("set") or {},
            "delete": parsed.get("delete") or [],
        }
        if not isinstance(patch["set"], dict):
            return None, f"'set' must be a mapping, got {type(patch['set']).__name__}"
        if not isinstance(patch["delete"], list):
            return (
                None,
                f"'delete' must be a list, got {type(patch['delete']).__name__}",
            )
        return patch, ""
    except Exception as e:
        return None, f"YAML parse error: {e}"


if __name__ == "__main__":
    base_cr = cassandra_datacenter = {
        "apiVersion": "cassandra.datastax.com/v1beta1",
        "kind": "CassandraDatacenter",
        "metadata": {"name": "test-cluster", "namespace": "default"},
        "spec": {
            "additionalAnnotations": {"example.com/annotation": "value"},
            "additionalLabels": {"example.com/label": "value"},
            "additionalSeeds": [],
            "additionalServiceConfig": {
                "additionalSeedService": {
                    "additionalAnnotations": {
                        "service.example.com/annotation": "value"
                    },
                    "additionalLabels": {"service.example.com/label": "value"},
                },
                "allpodsService": {
                    "additionalAnnotations": {
                        "service.example.com/annotation": "value"
                    },
                    "additionalLabels": {"service.example.com/label": "value"},
                },
                "dcService": {
                    "additionalAnnotations": {
                        "service.example.com/annotation": "value"
                    },
                    "additionalLabels": {"service.example.com/label": "value"},
                },
                "nodePortService": {
                    "additionalAnnotations": {
                        "service.example.com/annotation": "value"
                    },
                    "additionalLabels": {"service.example.com/label": "value"},
                },
                "seedService": {
                    "additionalAnnotations": {
                        "service.example.com/annotation": "value"
                    },
                    "additionalLabels": {"service.example.com/label": "value"},
                },
            },
            "allowMultipleNodesPerWorker": False,
            "canaryUpgrade": False,
            "canaryUpgradeCount": 0,
            "cdc": {
                "cdcConcurrentProcessors": 2,
                "cdcPollIntervalM": 5,
                "cdcWorkingDir": "/var/lib/cassandra/cdc",
                "errorCommitLogReprocessEnabled": False,
                "pulsarAuthParams": "param-value",
                "pulsarAuthPluginClassName": "org.apache.pulsar.client.impl.auth.AuthenticationToken",
                "pulsarBatchDelayInMs": 100,
                "pulsarKeyBasedBatcher": False,
                "pulsarMaxPendingMessages": 1000,
                "pulsarMaxPendingMessagesAcrossPartitions": 50000,
                "pulsarServiceUrl": "pulsar://pulsar-broker:6650",
                "sslAllowInsecureConnection": "false",
                "sslCipherSuites": "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                "sslEnabledProtocols": "TLSv1.2,TLSv1.3",
                "sslHostnameVerificationEnable": "true",
                "sslKeystorePassword": "changeit",
                "sslKeystorePath": "/path/to/keystore.jks",
                "sslProvider": "JDK",
                "sslTruststorePassword": "changeit",
                "sslTruststorePath": "/path/to/truststore.jks",
                "sslTruststoreType": "JKS",
                "topicPrefix": "persistent://public/default/cassandra-",
            },
            "clusterName": "development",
            "config": {
                "cassandra-yaml": {
                    "authenticator": "PasswordAuthenticator",
                    "authorizer": "CassandraAuthorizer",
                    "num_tokens": 16,
                    "role_manager": "CassandraRoleManager",
                }
            },
            "configBuilderImage": "datastax/cass-config-builder:1.0.4",
            "configBuilderResources": {
                "limits": {"cpu": "500m", "memory": "512Mi"},
                "requests": {"cpu": "250m", "memory": "256Mi"},
            },
            "datacenterName": "test-dc",
            "disableSystemLoggerSidecar": False,
            "dseWorkloads": {
                "analyticsEnabled": False,
                "graphEnabled": False,
                "searchEnabled": False,
            },
            "managementApiAuth": {"insecure": None},
            "minReadySeconds": 5,
            "networking": {
                "hostNetwork": False,
                "nodePort": {
                    "internode": 30001,
                    "internodeSSL": 30002,
                    "native": 30003,
                    "nativeSSL": 30004,
                },
            },
            "nodeAffinityLabels": {"node-type": "cassandra"},
            "nodeSelector": {"disktype": "ssd"},
            "podTemplateSpec": {
                "metadata": {
                    "annotations": {"pod.example.com/annotation": "value"},
                    "labels": {"pod.example.com/label": "value"},
                },
                "spec": {
                    "affinity": {
                        "nodeAffinity": {
                            "preferredDuringSchedulingIgnoredDuringExecution": [
                                {
                                    "preference": {
                                        "matchExpressions": [
                                            {
                                                "key": "topology.kubernetes.io/zone",
                                                "operator": "In",
                                                "values": ["zone-a"],
                                            }
                                        ]
                                    },
                                    "weight": 100,
                                }
                            ],
                            "requiredDuringSchedulingIgnoredDuringExecution": {
                                "nodeSelectorTerms": [
                                    {
                                        "matchExpressions": [
                                            {
                                                "key": "kubernetes.io/os",
                                                "operator": "In",
                                                "values": ["linux"],
                                            }
                                        ]
                                    }
                                ]
                            },
                        },
                        "podAffinity": {
                            "preferredDuringSchedulingIgnoredDuringExecution": [
                                {
                                    "podAffinityTerm": {
                                        "labelSelector": {
                                            "matchExpressions": [
                                                {
                                                    "key": "app",
                                                    "operator": "In",
                                                    "values": ["cassandra"],
                                                }
                                            ]
                                        },
                                        "topologyKey": "topology.kubernetes.io/zone",
                                    },
                                    "weight": 100,
                                }
                            ]
                        },
                        "podAntiAffinity": {
                            "requiredDuringSchedulingIgnoredDuringExecution": [
                                {
                                    "labelSelector": {
                                        "matchExpressions": [
                                            {
                                                "key": "app",
                                                "operator": "In",
                                                "values": ["cassandra"],
                                            }
                                        ]
                                    },
                                    "topologyKey": "kubernetes.io/hostname",
                                }
                            ]
                        },
                    },
                    "automountServiceAccountToken": True,
                    "containers": [
                        {
                            "env": [
                                {"name": "MAX_HEAP_SIZE", "value": "2048M"},
                                {
                                    "name": "POD_IP",
                                    "valueFrom": {
                                        "fieldRef": {"fieldPath": "status.podIP"}
                                    },
                                },
                            ],
                            "envFrom": [
                                {
                                    "configMapRef": {
                                        "name": "cassandra-env-config",
                                        "optional": True,
                                    }
                                }
                            ],
                            "lifecycle": {
                                "preStop": {
                                    "exec": {
                                        "command": ["/bin/sh", "-c", "nodetool drain"]
                                    }
                                }
                            },
                            "livenessProbe": {
                                "failureThreshold": 5,
                                "httpGet": {
                                    "path": "/api/v0/probes/liveness",
                                    "port": 8080,
                                },
                                "initialDelaySeconds": 30,
                                "periodSeconds": 15,
                                "timeoutSeconds": 5,
                            },
                            "name": "cassandra",
                            "readinessProbe": {
                                "failureThreshold": 3,
                                "httpGet": {
                                    "path": "/api/v0/probes/readiness",
                                    "port": 8080,
                                },
                                "initialDelaySeconds": 30,
                                "periodSeconds": 10,
                                "timeoutSeconds": 5,
                            },
                            "resources": {
                                "limits": {"cpu": "2000m", "memory": "4Gi"},
                                "requests": {"cpu": "1000m", "memory": "2Gi"},
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                                "runAsNonRoot": True,
                                "runAsUser": 999,
                                "seccompProfile": {"type": "RuntimeDefault"},
                            },
                            "startupProbe": {
                                "failureThreshold": 30,
                                "httpGet": {
                                    "path": "/api/v0/probes/liveness",
                                    "port": 8080,
                                },
                                "initialDelaySeconds": 60,
                                "periodSeconds": 10,
                                "timeoutSeconds": 5,
                            },
                            "volumeMounts": [
                                {
                                    "mountPath": "/var/lib/cassandra/commitlog",
                                    "name": "commitlog",
                                }
                            ],
                        }
                    ],
                    "dnsConfig": {"options": [{"name": "ndots", "value": "5"}]},
                    "dnsPolicy": "ClusterFirst",
                    "enableServiceLinks": False,
                    "initContainers": [
                        {
                            "command": ["sh", "-c", "echo 'init container'"],
                            "image": "busybox:latest",
                            "name": "init-config",
                        }
                    ],
                    "securityContext": {
                        "fsGroup": 999,
                        "runAsGroup": 999,
                        "runAsNonRoot": True,
                        "runAsUser": 999,
                    },
                    "terminationGracePeriodSeconds": 120,
                },
            },
            "racks": [
                {
                    "name": "rack1",
                    "nodeAffinityLabels": {"rack": "rack1"},
                    "zone": "us-east-1a",
                }
            ],
            "readOnlyRootFilesystem": False,
            "resources": {
                "limits": {"cpu": "2000m", "memory": "4Gi"},
                "requests": {"cpu": "1000m", "memory": "2Gi"},
            },
            "rollingRestartRequested": False,
            "serverType": "dse",
            "serverVersion": "6.8.0",
            "serviceAccountName": "default",
            "size": 1,
            "stopped": True,
            "storageConfig": {
                "additionalVolumes": [
                    {
                        "mountPath": "/var/lib/cassandra/commitlog",
                        "name": "commitlog",
                        "pvcSpec": {
                            "accessModes": ["ReadWriteOnce"],
                            "resources": {"requests": {"storage": "5Gi"}},
                            "storageClassName": "standard",
                        },
                    }
                ],
                "cassandraDataVolumeClaimSpec": {
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {"requests": {"storage": "10Gi"}},
                    "storageClassName": "standard",
                },
            },
            "systemLoggerImage": "busybox:latest",
            "systemLoggerResources": {
                "limits": {"cpu": "100m", "memory": "128Mi"},
                "requests": {"cpu": "50m", "memory": "64Mi"},
            },
            "tolerations": [
                {
                    "effect": "NoSchedule",
                    "key": "dedicated",
                    "operator": "Equal",
                    "value": "cassandra",
                }
            ],
        },
    }
    patch = {
        "set": {"spec.racks": []},
        "delete": [],
    }
    base_cr = _apply_patch_to_cr(base_cr, patch)
    patch2 = {
        "set": {"spec.racks[*].name": "rack1"},
        "delete": [],
    }
    base_cr = _apply_patch_to_cr(base_cr, patch2)

    data_str = yaml.dump(base_cr, default_flow_style=False, sort_keys=False)
    print(data_str)