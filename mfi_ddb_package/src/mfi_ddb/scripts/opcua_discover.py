"""
Browse an OPC UA server and print (or save) the variables the OPC UA data adapter would
auto-discover, as a ready-to-edit adapter config with an explicit `nodes` list.

Example usage:
$ python -m mfi_ddb.scripts.opcua_discover --endpoint opc.tcp://192.168.1.10:4840
$ python -m mfi_ddb.scripts.opcua_discover --endpoint opc.tcp://host:4840 \
      --root "ns=2;s=Line1" --component-depth 2 --exclude Diagnostics -o opcua_nodes.yaml
"""

import argparse

import yaml

from mfi_ddb.data_adapters.opcua import OpcuaDataAdapter


def main():
    parser = argparse.ArgumentParser(
        description="Auto-discover OPC UA variables for the OPC UA data adapter.",
        epilog=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--endpoint", required=True, help="opc.tcp://host:port[/path]")
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--security-string", help="'Policy,Mode,cert,key[,server_cert]'")
    parser.add_argument("--root", default="i=85", help="Node to browse from (default: Objects)")
    parser.add_argument("--component-depth", type=int, default=1)
    parser.add_argument("--max-depth", type=int, default=10)
    parser.add_argument("--max-nodes", type=int, default=1000)
    parser.add_argument("--namespace", action="append", help="Namespace index/URI (repeatable)")
    parser.add_argument("--include", action="append", default=[], help="Regex (repeatable)")
    parser.add_argument("--exclude", action="append", default=[], help="Regex (repeatable)")
    parser.add_argument("--trial-id", default="trial_001")
    parser.add_argument("--output", "-o", help="Write the config YAML here instead of stdout")
    args = parser.parse_args()

    opcua_cfg = {"endpoint": args.endpoint}
    if args.username:
        opcua_cfg["username"] = args.username
    if args.password:
        opcua_cfg["password"] = args.password
    if args.security_string:
        opcua_cfg["security_string"] = args.security_string

    discovery = {
        "root": args.root,
        "component_depth": args.component_depth,
        "max_depth": args.max_depth,
        "max_nodes": args.max_nodes,
        "include": args.include,
        "exclude": args.exclude,
    }
    if args.namespace:
        discovery["namespaces"] = [int(n) if n.isdigit() else n for n in args.namespace]

    nodes = OpcuaDataAdapter.discover({"opcua": opcua_cfg, "discovery": discovery})

    components = sorted({n["component_id"] for n in nodes})
    summary = f"# Discovered {len(nodes)} variables in {len(components)} components\n"
    opcua_cfg.pop("password", None)
    config = {
        "adapter_name": "my_opcua_adapter",
        "trial_id": args.trial_id,
        "opcua": opcua_cfg,
        "nodes": nodes,
    }
    text = summary + yaml.dump(config, sort_keys=False)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(text)
        print(summary.strip("# \n") + f" -> {args.output}")
    else:
        print(text)


if __name__ == "__main__":
    main()
