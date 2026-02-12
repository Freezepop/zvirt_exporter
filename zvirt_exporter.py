#!/usr/bin/python3

import json
import time
import logging
import asyncio
import aiohttp
from os import getenv
from fastapi import FastAPI, Response
from threading import Lock
logger = logging.getLogger(__name__)
log = logging.getLogger("zvirt_exporter")


VIRT_SCHEME = getenv("VIRT_SCHEME", "")
VIRT_URL = getenv("VIRT_URL", "")
USERNAME = getenv("USERNAME", "")
PASSWORD = getenv("PASSWORD", "")
DOMAIN = getenv("DOMAIN", "")

TOKEN_CACHE = {"access_token": None}
TOKEN_LOCK = Lock()
METRICS_CACHE = {"data": None,
                 "timestamp": 0}
CACHE_TTL = 5
CACHE_LOCK = Lock()

user = f"{USERNAME}@{DOMAIN}"
password = PASSWORD

app = FastAPI()


async def get_token(session):

    headers = {"Accept": "application/json"}

    with TOKEN_LOCK:
        access_token = TOKEN_CACHE["access_token"]

    if access_token:
        url = f"{VIRT_SCHEME}://{VIRT_URL}/ovirt-engine/api/vms?max=1"
        async with session.get(url, headers={**headers, "Authorization": f"Bearer {access_token}"}, ssl=False) as resp:
            if resp.status != 401:
                return access_token

    url = f"{VIRT_SCHEME}://{VIRT_URL}/ovirt-engine/sso/oauth/token"
    params = {
        "grant_type": "password",
        "scope": "ovirt-app-api",
        "username": user,
        "password": password
    }

    async with session.post(url, params=params, headers={**headers, "Content-Type": "application/x-www-form-urlencoded"}, ssl=False) as resp:
        resp.raise_for_status()
        data = await resp.json()
        new_token = data["access_token"]

        with TOKEN_LOCK:
            TOKEN_CACHE["access_token"] = new_token

        return new_token


async def get_vm_statistics(session, token):
    url = f"{VIRT_SCHEME}://{VIRT_URL}/ovirt-engine/api/vms?follow=statistics,disk_attachments.disk.statistics,nics.statistics,snapshots.disks.statistics,tags"
    async with session.get(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                           ssl=False) as resp:
        vm_statistics = await resp.json()

        lines = []

        for vm in vm_statistics["vm"]:
            labels = {"object_type": "vm",
                      "fqdn": vm.get("fqdn", "unknown"),
                      "name": vm.get("name", "unknown"),
                      "id": vm.get("id", "unknown"),
                      "ip": vm.get("display", {}).get("address", "unknown"),
                      "os_architecture": vm.get("guest_operating_system", {}).get("architecture", "unknown"),
                      "codename": vm.get("guest_operating_system", {}).get("codename", "unknown"),
                      "distribution": vm.get("guest_operating_system", {}).get("distribution", "unknown"),
                      "family": vm.get("guest_operating_system", {}).get("family", "unknown"),
                      "kernel_build": vm.get("guest_operating_system", {}).get("kernel", {}).get("version", {}).get("build", "unknown"),
                      "kernel_full_version": vm.get("guest_operating_system", {}).get("kernel", {}).get("version", {}).get("full_version", "unknown"),
                      "kernel_major": vm.get("guest_operating_system", {}).get("kernel", {}).get("version", {}).get("major", "unknown"),
                      "kernel_minor": vm.get("guest_operating_system", {}).get("kernel", {}).get("version", {}).get("minor", "unknown"),
                      "kernel_revision": vm.get("guest_operating_system", {}).get("kernel", {}).get("version", {}).get("revision", "unknown"),
                      "distribution_full_version": vm.get("guest_operating_system", {}).get("version", {}).get("full_version", "unknown"),
                      "distribution_major": vm.get("guest_operating_system", {}).get("version", {}).get("major", "unknown"),
                      "distribution_minor": vm.get("guest_operating_system", {}).get("version", {}).get("minor", "unknown"),
                      "distribution_revision": vm.get("guest_operating_system", {}).get("version", {}).get("revision", "unknown"),
                      "time_zone": vm.get("time_zone", {}).get("name", "unknown"),
                      "guest_time_zone_name": vm.get("guest_time_zone", {}).get("name", "unknown"),
                      "guest_time_zone_utc_offset": vm.get("guest_time_zone", {}).get("utc_offset", "unknown"),
                      "bios_type": vm.get('bios', {}).get("type", "unknown"),
                      "cpu_architecture": vm.get('cpu', {}).get("architecture", "unknown"),
                      "template_id": vm.get("template", {}).get("id", "unknown"),
                      "cluster_id": vm.get("cluster", {}).get("id", "unknown"),
                      "quota_id": vm.get("quota", {}).get("id", "unknown"),
                      "cpu_profile_id": vm.get("cpu_profile", {}).get("id", "unknown")}

            cmdb_tags = {"CMDB_AS_ID": "unknown", "CMDB_GAS_ID": "unknown", "CMDB_ENV": "unknown", "CMDB_CRIT": "unknown"}

            for item in vm.get("tags", {}).get("tag", {}):
                tag_data = item.get("name", "").split(".")
                tag_data_len = len(tag_data)
                if tag_data_len == 2:
                    if tag_data[0] == "CMDB_AS_ID":
                        cmdb_tags["CMDB_AS_ID"] = tag_data[1]
                    elif tag_data[0] == "CMDB_GAS_ID":
                        cmdb_tags["CMDB_GAS_ID"] = tag_data[1]
                    elif tag_data[0] == "CMDB_ENV":
                        cmdb_tags["CMDB_ENV"] = tag_data[1]
                    elif tag_data[0] == "CMDB_CRIT":
                        cmdb_tags["CMDB_CRIT"] = tag_data[1]
                    else:
                        cmdb_tags[tag_data[0]] = tag_data[1]
                elif tag_data_len == 1:
                    if len(tag_data[0]) > 0:
                        cmdb_tags[tag_data[0]] = "unknown"

            labels = {**labels, **cmdb_tags}

            labels = ", ".join(f'{k}="{v}"' for k, v in labels.items())
            lines.append("# HELP next_run_configuration_exists Are there any configuration changes made to the VM that are pending confirmation (bool).\n")
            lines.append("# TYPE next_run_configuration_exists gauge\n")
            lines.append(f"next_run_configuration_exists{{{labels}}} {1 if vm.get('next_run_configuration_exists', 'false') == 'true' else 0}\n")
            lines.append("# HELP run_once Is VM Run Once (bool).\n")
            lines.append("# TYPE run_once gauge\n")
            lines.append(f"run_once{{{labels}}} {1 if vm.get('run_once', 'false') == 'true' else 0}\n")
            lines.append("# HELP creation_time VM creation date (timestamp).\n")
            lines.append("# TYPE creation_time gauge\n")
            lines.append(f"creation_time{{{labels}}} {vm.get('creation_time', 0)}\n")
            lines.append("# HELP start_time VM start date (timestamp).\n")
            lines.append("# TYPE start_time gauge\n")
            lines.append(f"start_time{{{labels}}} {vm.get('start_time', 0)}\n")
            lines.append("# HELP stop_time VM stop date (timestamp).\n")
            lines.append("# TYPE stop_time gauge\n")
            lines.append(f"stop_time{{{labels}}} {vm.get('stop_time', 0)}\n")
            lines.append("# HELP status VM status (bool).\n")
            lines.append("# TYPE status gauge\n")
            lines.append(f"status{{{labels}}} {1 if vm.get('status', 'down') == 'up' else 0}\n")
            lines.append("# HELP boot_menu_enabled Is the VM boot menu enabled (bool).\n")
            lines.append("# TYPE boot_menu_enabled gauge\n")
            lines.append(f"boot_menu_enabled{{{labels}}} {1 if vm.get('bios', {}).get('boot_menu', {}).get('enabled', 'false') == 'true' else 0}\n")
            lines.append("# HELP cpu_mode Current CPU mode: 0/1/2/3 - custom/host_model/host_passthrough/unknown (number).\n")
            lines.append("# TYPE cpu_mode gauge\n")
            lines.append(f"cpu_mode{{{labels}}} { {'custom': 0, 'host_model': 1, 'host_passthrough': 2, 'unknown': 3}.get(vm.get('cpu', {}).get('mode', 'unknown'))}\n")
            lines.append("# HELP cpu_topology_cores Number of VM CPU cores (number).\n")
            lines.append("# TYPE cpu_topology_cores gauge\n")
            lines.append(f"cpu_topology_cores{{{labels}}} {vm.get('cpu', {}).get('topology', {}).get('cores', 0)}\n")
            lines.append("# HELP cpu_topology_sockets Number of VM CPU sockets (number).\n")
            lines.append("# TYPE cpu_topology_sockets gauge\n")
            lines.append(f"cpu_topology_sockets{{{labels}}} {vm.get('cpu', {}).get('topology', {}).get('sockets', 0)}\n")
            lines.append("# HELP cpu_topology_threads Number of VM CPU threads (number).\n")
            lines.append("# TYPE cpu_topology_threads gauge\n")
            lines.append(f"cpu_topology_threads{{{labels}}} {vm.get('cpu', {}).get('topology', {}).get('threads', 0)}\n")
            lines.append("# HELP placement_policy_affinity The configuration of the virtual machine’s placement policy: 0/1/2/3 - migratable/pinned/user_migratable/unknown (number).\n")
            lines.append("# TYPE placement_policy_affinity gauge\n")
            lines.append(f"placement_policy_affinity{{{labels}}} { {'migratable': 0, 'pinned': 1, 'user_migratable': 2, 'unknown': 3}.get(vm.get('placement_policy', {}).get('affinity', 'unknown'))}\n")
            lines.append("# HELP storage_error_resume_behaviour Determines how the virtual machine will be resumed after storage error: 0/1/2/3 - auto_resume/kill/leave_paused/unknown (number).\n")
            lines.append("# TYPE storage_error_resume_behaviour gauge\n")
            lines.append(f"storage_error_resume_behaviour{{{labels}}} { {'auto_resume': 0, 'kill': 1, 'leave_paused': 2, 'unknown': 3}.get(vm.get('storage_error_resume_behaviour', 'unknown'))}\n")
            lines.append("# HELP io_threads Number of I/O threads. VirtIO disks are pinned to an I/O thread using a round-robin algorithm (number).\n")
            lines.append("# TYPE io_threads gauge\n")
            lines.append(f"io_threads{{{labels}}} {vm.get('io', {}).get('threads', 0)}\n")
            lines.append("# HELP memory Assigned memory during configuration (bytes).\n")
            lines.append("# TYPE memory gauge\n")
            lines.append(f"memory{{{labels}}} {vm.get('memory', 0)}\n")
            lines.append("# HELP stateless VM is stateless (bool)?\n")
            lines.append("# TYPE stateless gauge\n")
            lines.append(f"stateless{{{labels}}} {1 if vm.get('stateless', 'false') == 'true' else 0}\n")
            lines.append("# HELP usb_enabled VM USB is enabled (bool)?\n")
            lines.append("# TYPE usb_enabled gauge\n")
            lines.append(f"usb_enabled{{{labels}}} {1 if vm.get('usb', {}).get('enabled', 'false') == 'true' else 0}\n")
            lines.append("# HELP cpu_shares CPU shares weight (0 == auto) (number).\n")
            lines.append("# TYPE cpu_shares gauge\n")
            lines.append(f"cpu_shares{{{labels}}} {vm.get('cpu_shares', 0)}\n")
            lines.append("# HELP delete_protected Is the VM protected from deletion (bool).\n")
            lines.append("# TYPE delete_protected gauge\n")
            lines.append(f"delete_protected{{{labels}}} {1 if vm.get('delete_protected', 'false') == 'true' else 0}\n")
            lines.append("# HELP high_availability_enabled VM HA is enabled (bool).\n")
            lines.append("# TYPE high_availability_enabled gauge\n")
            lines.append(f"high_availability_enabled{{{labels}}} {1 if vm.get('high_availability', {}).get('enabled', 'false') == 'true' else 0}\n")
            lines.append("# HELP high_availability_priority VM HA priority (number).\n")
            lines.append("# TYPE high_availability_priority gauge\n")
            lines.append(f"high_availability_priority{{{labels}}} {vm.get('high_availability', {}).get('priority', 0)}\n")
            lines.append("# HELP memory_policy_ballooning This parameter enables the memory balancing device for the virtual machine. For this device to work, memory overcommitment must be enabled in the cluster (bool).\n")
            lines.append("# TYPE memory_policy_ballooning gauge\n")
            lines.append(f"memory_policy_ballooning{{{labels}}} {1 if vm.get('memory_policy', {}).get('ballooning', 'false') == 'true' else 0}\n")
            lines.append("# HELP memory_policy_guaranteed VM guaranteed memory (bytes).\n")
            lines.append("# TYPE memory_policy_guaranteed gauge\n")
            lines.append(f"memory_policy_guaranteed{{{labels}}} {vm.get('memory_policy', {}).get('guaranteed', 0)}\n")
            lines.append("# HELP memory_policy_max VM max memory (bytes).\n")
            lines.append("# TYPE memory_policy_max gauge\n")
            lines.append(f"memory_policy_max{{{labels}}} {vm.get('memory_policy', {}).get('max', 0)}\n")
            lines.append("# HELP migration_downtime Max allowed VM downtime during live migration (-1 == cluster default) (number).\n")
            lines.append("# TYPE migration_downtime gauge\n")
            lines.append(f"migration_downtime{{{labels}}} {vm.get('migration_downtime', 0)}\n")
            lines.append("# HELP multi_queues_enabled This setting allows multiple queues. You can create up to four queues on each virtual network card, depending on the number of available vCPUs (bool).\n")
            lines.append("# TYPE multi_queues_enabled gauge\n")
            lines.append(f"multi_queues_enabled{{{labels}}} {1 if vm.get('multi_queues_enabled', 'false') == 'true' else 0}\n")
            lines.append("# HELP start_paused 1 if start paused VM enabled (bool).\n")
            lines.append("# TYPE start_paused gauge\n")
            lines.append(f"start_paused{{{labels}}} {1 if vm.get('start_paused', 'false') == 'true' else 0}\n")
            lines.append("# HELP virtio_scsi_multi_queues_enabled 1 if multiqueue virtio-scsi enabled (bool).\n")
            lines.append("# TYPE virtio_scsi_multi_queues_enabled gauge\n")
            lines.append(f"virtio_scsi_multi_queues_enabled{{{labels}}} {1 if vm.get('virtio_scsi_multi_queues_enabled', 'false') == 'true' else 0}\n")

            for item in vm.get("statistics", {}).get("statistic", {}):
                if ".history" not in item.get("name", "unknown"):
                    if item.get("type", "unknown") != "string":
                        lines.append(f"# HELP {item.get('name', 'unknown').replace('.', '_')} {item.get('description', 'unknown')} ({item.get('unit', 'unknown')}).\n")
                        lines.append(f"# TYPE {item.get('name', 'unknown').replace('.', '_')} {item.get('kind', 'unknown')}\n")
                        lines.append(f"{item.get('name', 'unknown').replace('.', '_')}{{{labels}}} {item.get('values', {}).get('value', {})[0].get('datum', 0)
                        if len(item.get('values', {})) > 0
                        else 0}\n")
                    else:
                        load_str_stats = json.loads(item.get("values", {}).get("value", {})[0].get("detail", 0)
                                                    if len(item.get('values', {})) > 0
                                                    else "[]")
                        for str_stat in load_str_stats:
                            labels_str_stats = f'{labels}, path="{str_stat["path"]}", fs="{str_stat["fs"]}"'
                            lines.append(f"# HELP {item.get('name', 'unknown').replace('.', '_')} Disk total space ({str_stat["path"]}, {str_stat["fs"]}) ({item.get('unit', 'unknown')}).\n")
                            lines.append(f"# TYPE {item.get('name', 'unknown').replace('.', '_')} gauge\n")
                            lines.append(f"fs_total{{{labels_str_stats}}} {str_stat['total']}\n")
                            lines.append(f"# HELP {item.get('name', 'unknown').replace('.', '_')} Disk space used ({str_stat["path"]}, {str_stat["fs"]}) ({item.get('unit', 'unknown')}).\n")
                            lines.append(f"# TYPE {item.get('name', 'unknown').replace('.', '_')} gauge\n")
                            lines.append(f"fs_used{{{labels_str_stats}}} {str_stat['used']}\n")
                            lines.append(f"# HELP {item.get('name', 'unknown').replace('.', '_')} Disk space used ({str_stat["path"]}, {str_stat["fs"]}) (percent).\n")
                            lines.append(f"# TYPE {item.get('name', 'unknown').replace('.', '_')} gauge\n")
                            lines.append(f"fs_percentage{{{labels_str_stats}}} {float((int(str_stat['used']) / int(str_stat['total'])) * 100):.2f}\n")

            for item in vm.get("nics", {}).get("nic", {}):
                labels_str_stats = (f'{labels}, interface="{item.get("interface", "unknown")}", '
                                    f'nic_mac="{item.get("mac", {}).get("address", "unknown")}", '
                                    f'nic_profile_id="{item.get("vnic_profile", {}).get("id", "unknown")}", '
                                    f'nic_name="{item.get("name", "unknown")}", '
                                    f'nic_id="{item.get("id", "unknown")}"')

                lines.append(f"# HELP plugged 1 if the VM network interface is plugged (attached) to the VM, else 0 (bool).\n")
                lines.append(f"# TYPE plugged gauge\n")
                lines.append(f"plugged{{{labels_str_stats}}} {1 if item.get('plugged', 'false') == 'true' else 0}\n")
                lines.append(f"# HELP synced 1 if the VM network interface configuration is fully synced with next-run settings, else 0 (bool).\n")
                lines.append(f"# TYPE synced gauge\n")
                lines.append(f"synced{{{labels_str_stats}}} {1 if item.get('synced', 'false') == 'true' else 0}\n")

                for nic_item in item.get("statistics", {}).get("statistic", {}):
                    lines.append(f"# HELP {nic_item.get('name', 'unknown').replace('.', '_')} {nic_item.get('description', 'unknown')} ({nic_item.get('unit', 'unknown')}).\n")
                    lines.append(f"# TYPE {nic_item.get('name', 'unknown').replace('.', '_')} {nic_item.get('kind', 'unknown')}\n")
                    lines.append(f"{nic_item.get('name', 'unknown').replace('.', '_')}{{{labels_str_stats}}} {nic_item.get('values', {}).get('value', {})[0].get('datum', 0)
                    if len(nic_item.get('values', {})) > 0
                    else 0}\n")

            for item in vm.get("disk_attachments", {}).get("disk_attachment", {}):
                labels_str_stats = (f'{labels}, logical_name="{item.get("logical_name", "unknown")}", '
                                    f'alias="{item.get("disk", {}).get('alias', "unknown")}", '
                                    f'disk_name="{item.get("disk", {}).get('name', "unknown")}", '
                                    f'disk_id="{item.get("disk", {}).get('id', "unknown")}", '
                                    f'image_id="{item.get("disk", {}).get('image_id', "unknown")}", '
                                    f'disk_profile_id="{item.get("disk", {}).get('disk_profile', {}).get("id", "unknown")}", '
                                    f'quota_id="{item.get("disk", {}).get('quota', {}).get("id", "unknown")}", '
                                    f'storage_domain_id="{item.get("disk", {}).get('storage_domains', {}).get("storage_domain", {})[0].get("id", "unknown")}"')

                lines.append(f"# HELP interface The type of interface driver used to connect the disk device to the virtual machine: 0/1/2/3/4/5 - ide/sata/spapr_vscsi/virtio/virtio_scsi/unknown (number).\n")
                lines.append(f"# TYPE interface gauge\n")
                lines.append(f"interface{{{labels_str_stats}}} { {'ide': 0, 'sata': 1, 'spapr_vscsi': 2, 'virtio': 3, 'virtio_scsi': 4, 'unknown': 5}.get(item.get('interface', 'unknown'))}\n")
                lines.append(f"# HELP disk_backup The backup behavior supported by the disk: 0/1/2 - incremental/none/unknown (number).\n")
                lines.append(f"# TYPE disk_backup gauge\n")
                lines.append(f"disk_backup{{{labels_str_stats}}} { {'incremental': 0, 'none': 1, 'unknown': 2}.get(item.get('disk', {}).get('backup', 'unknown'))}\n")
                lines.append(f"# HELP disk_content_type Indicates the actual content residing on the disk: 0/1/2/3/4/5/6/7/8/9/10 - backup_scratch/data/hosted_engine/hosted_engine_configuration/hosted_engine_metadata/hosted_engine_sanlock/iso/memory_dump_volume/memory_metadata_volume/ovf_store/unknown (number).\n")
                lines.append(f"# TYPE disk_content_type gauge\n")
                lines.append(f"disk_content_type{{{labels_str_stats}}} { {'backup_scratch': 0, 'data': 1, 'hosted_engine': 2,
                                                                          'hosted_engine_configuration': 3, 'hosted_engine_metadata': 4, 'hosted_engine_sanlock': 5,
                                                                          'iso': 6, 'memory_dump_volume': 7, 'memory_metadata_volume': 8,
                                                                          'ovf_store': 9, 'unknown': 10}.get(item.get('disk', {}).get('content_type', 'unknown'))}\n")
                lines.append(f"# HELP disk_format The underlying storage format: 0/1/2 - cow/raw/unknown (number).\n")
                lines.append(f"# TYPE disk_format gauge\n")
                lines.append(f"disk_format{{{labels_str_stats}}} { {'cow': 0, 'raw': 1, 'unknown': 2}.get(item.get('disk', {}).get('format', 'unknown'))}\n")
                lines.append(f"# HELP disk_qcow_version The underlying QCOW version of a QCOW volume: 0/1/2 - qcow2_v2/qcow2_v3/unknown (number).\n")
                lines.append(f"# TYPE disk_qcow_version gauge\n")
                lines.append(f"disk_qcow_version{{{labels_str_stats}}} { {'qcow2_v2': 0, 'qcow2_v3': 1, 'unknown': 2}.get(item.get('disk', {}).get('qcow_version', 'unknown'))}\n")
                lines.append(f"# HELP disk_storage_type Disk storage type: 0/1/2/3/4 - cinder/image/lun/managed_block_storage/unknown (number).\n")
                lines.append(f"# TYPE disk_storage_type gauge\n")
                lines.append(f"disk_storage_type{{{labels_str_stats}}} { {'cinder': 0, 'image': 1, 'lun': 2, 'managed_block_storage': 3,'unknown': 4}.get(item.get('disk', {}).get('storage_type', 'unknown'))}\n")
                lines.append(f"# HELP active 1 if the disk is currently active (attached and in case), else 0 (bool).\n")
                lines.append(f"# TYPE active gauge\n")
                lines.append(f"active{{{labels_str_stats}}} {1 if item.get('active', 'false') == 'true' else 0}\n")
                lines.append(f"# HELP bootable 1 if the disk is marked as bootable for the VM, else 0 (bool).\n")
                lines.append(f"# TYPE bootable gauge\n")
                lines.append(f"bootable{{{labels_str_stats}}} {1 if item.get('bootable', 'false') == 'true' else 0}\n")
                lines.append(f"# HELP pass_discard 1 if discard/UNMAP/TRIM operations from the guest are passed to storage, else 0 (bool).\n")
                lines.append(f"# TYPE pass_discard gauge\n")
                lines.append(f"pass_discard{{{labels_str_stats}}} {1 if item.get('pass_discard', 'false') == 'true' else 0}\n")
                lines.append(f"# HELP read_only 1 if the disk is attached as read-only, else 0 (bool).\n")
                lines.append(f"# TYPE read_only gauge\n")
                lines.append(f"read_only{{{labels_str_stats}}} {1 if item.get('read_only', 'false') == 'true' else 0}\n")
                lines.append(f"# HELP uses_scsi_reservation 1 if SCSI reservations are enabled for this disk, else 0 (bool).\n")
                lines.append(f"# TYPE uses_scsi_reservation gauge\n")
                lines.append(f"uses_scsi_reservation{{{labels_str_stats}}} {1 if item.get('uses_scsi_reservation', 'false') == 'true' else 0}\n")
                lines.append(f"# HELP actual_size Actual allocated size of the disk on storage (bytes).\n")
                lines.append(f"# TYPE actual_size gauge\n")
                lines.append(f"actual_size{{{labels_str_stats}}} {item.get("disk", {}).get('actual_size', 0)}\n")
                lines.append(f"# HELP propagate_errors 1 if disk I/O errors propagate to the guest (fatal), else 0 (bool).\n")
                lines.append(f"# TYPE propagate_errors gauge\n")
                lines.append(f"propagate_errors{{{labels_str_stats}}} {1 if item.get("disk", {}).get('propagate_errors', 'false') == 'true' else 0}\n")
                lines.append(f"# HELP provisioned_size Provisioned (virtual) size of the disk (bytes).\n")
                lines.append(f"# TYPE provisioned_size gauge\n")
                lines.append(f"provisioned_size{{{labels_str_stats}}} {item.get("disk", {}).get('provisioned_size', 0)}\n")
                lines.append(f"# HELP shareable 1 if the disk is marked as shareable between VMs, else 0 (bool).\n")
                lines.append(f"# TYPE shareable gauge\n")
                lines.append(f"shareable{{{labels_str_stats}}} {1 if item.get("disk", {}).get('shareable', 'false') == 'true' else 0}\n")
                lines.append(f"# HELP sparse 1 if the disk is thin-provisioned (sparse), else 0 (bool).\n")
                lines.append(f"# TYPE sparse gauge\n")
                lines.append(f"sparse{{{labels_str_stats}}} {1 if item.get("disk", {}).get('sparse', 'false') == 'true' else 0}\n")
                lines.append(f"# HELP status 1 if the disk status is ok, else 0 (bool).\n")
                lines.append(f"# TYPE status gauge\n")
                lines.append(f"status{{{labels_str_stats}}} {1 if item.get("disk", {}).get('status', 'fail') == 'ok' else 0}\n")
                lines.append(f"# HELP total_size Total space consumed by the disk on storage (bytes).\n")
                lines.append(f"# TYPE total_size gauge\n")
                lines.append(f"total_size{{{labels_str_stats}}} {item.get("disk", {}).get('total_size', 0)}\n")
                lines.append(f"# HELP wipe_after_delete 1 if secure wipe after delete is enabled, else 0 (bool).\n")
                lines.append(f"# TYPE wipe_after_delete gauge\n")
                lines.append(f"wipe_after_delete{{{labels_str_stats}}} {1 if item.get("disk", {}).get('wipe_after_delete', 'false') == 'true' else 0}\n")

                for disk_item in item.get("statistics", {}).get("statistic", {}):
                    lines.append(f"# HELP {disk_item.get('name', 'unknown').replace('.', '_')} {disk_item.get('description', 'unknown')} ({disk_item.get('unit', 'unknown')}).\n")
                    lines.append(f"# TYPE {disk_item.get('name', 'unknown').replace('.', '_')} {disk_item.get('kind', 'unknown')}\n")
                    lines.append(f"{disk_item.get('name', 'unknown').replace('.', '_')}{{{labels_str_stats}}} {disk_item.get('values', {}).get('value', {})[0].get('datum', 0)
                    if len(disk_item.get('values', {})) > 0
                    else 0}\n")

            for item in vm.get("snapshots", {}).get("snapshot", {}):
                if len(item.get("disks", {}).get("disk", {})) > 0:
                    for snap_item in item.get("disks", {}).get("disk", {}):
                        labels_str_stats = (f'{labels}, snapshot_id="{snap_item.get("snapshot", {}).get("id", "unknown")}", '
                                            f'alias="{snap_item.get('alias', "unknown")}", '
                                            f'backup="{snap_item.get('backup', "unknown")}", '
                                            f'content_type="{snap_item.get('content_type', "unknown")}", '
                                            f'format="{snap_item.get('format', "unknown")}", '
                                            f'image_id="{snap_item.get('image_id', "unknown")}", '
                                            f'storage_type="{snap_item.get('storage_type', "unknown")}", '
                                            f'disk_profile_id="{snap_item.get("disk", {}).get('disk_profile', {}).get("id", "unknown")}", '
                                            f'quota_id="{snap_item.get("disk", {}).get('quota', {}).get("id", "unknown")}", '
                                            f'storage_domain_id="{snap_item.get('storage_domains', {}).get("storage_domain", {})[0].get("id", "unknown")}"')

                        lines.append(f"date{{{labels_str_stats}}} {item.get('date')}\n")
                        lines.append(f"persist_memorystate{{{labels_str_stats}}} {1 if item.get('persist_memorystate', 'false') == 'true' else 0}\n")
                        lines.append(f"snapshot_status{{{labels_str_stats}}} {1 if item.get('snapshot_status', 'fail') == 'ok' else 0}\n")
                        lines.append(f"snapshot_type{{{labels_str_stats}}} {1 if item.get('snapshot_type', 'inactive') == 'active' else 0}\n")
                        lines.append(f"actual_size{{{labels_str_stats}}} {snap_item.get('actual_size', 0)}\n")
                        lines.append(f"propagate_errors{{{labels_str_stats}}} {1 if snap_item.get('propagate_errors', 'false') == 'true' else 0}\n")
                        lines.append(f"provisioned_size{{{labels_str_stats}}} {snap_item.get('provisioned_size', 0)}\n")
                        lines.append(f"shareable{{{labels_str_stats}}} {1 if snap_item.get('shareable', 'false') == 'true' else 0}\n")
                        lines.append(f"sparse{{{labels_str_stats}}} {1 if snap_item.get('sparse', 'false') == 'true' else 0}\n")
                        lines.append(f"status{{{labels_str_stats}}} {1 if snap_item.get('status', 'fail') == 'ok' else 0}\n")
                        lines.append(f"total_size{{{labels_str_stats}}} {snap_item.get('total_size', 0)}\n")
                        lines.append(f"wipe_after_delete{{{labels_str_stats}}} {1 if snap_item.get('wipe_after_delete', 'false') == 'true' else 0}\n")

        return lines


async def get_hosts_statistics(session, token):
    url = f"{VIRT_SCHEME}://{VIRT_URL}/ovirt-engine/api/hosts?follow=statistics,nics.statistics,tags"
    async with session.get(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                           ssl=False) as resp:
        hosts_statistics = await resp.json()

        lines = []

        for host in hosts_statistics["host"]:
            labels = {"object_type": "host",
                      "address": host.get("address", "unknown"),
                      "name": host.get("name", "unknown"),
                      "id": host.get("id", "unknown"),
                      "certificate_organization": host.get("certificate", {}).get("organization", "unknown"),
                      "certificate_subject": host.get("certificate", {}).get("subject", "unknown"),
                      "cpu_name": host.get("cpu", {}).get("name", "unknown"),
                      "cpu_type": host.get("cpu", {}).get("type", "unknown"),
                      "hardware_information_family": host.get("hardware_information", {}).get("family", "unknown"),
                      "hardware_information_manufacturer": host.get("hardware_information", {}).get("manufacturer", "unknown"),
                      "hardware_information_product_name": host.get("hardware_information", {}).get("product_name", "unknown"),
                      "hardware_information_serial_number": host.get("hardware_information", {}).get("serial_number", "unknown"),
                      "hardware_information_uuid": host.get("hardware_information", {}).get("uuid", "unknown"),
                      "hardware_information_version": host.get("hardware_information", {}).get("version", "unknown"),
                      "iscsi_initiator": host.get("iscsi", {}).get("initiator", "unknown"),
                      "libvirt_version_build": host.get("libvirt_version", {}).get("build", "unknown"),
                      "libvirt_version_full_version": host.get("libvirt_version", {}).get("full_version", "unknown"),
                      "libvirt_version_major": host.get("libvirt_version", {}).get("major", "unknown"),
                      "libvirt_version_minor": host.get("libvirt_version", {}).get("minor", "unknown"),
                      "libvirt_version_revision": host.get("libvirt_version", {}).get("revision", "unknown"),
                      "os_type": host.get("os", {}).get("type", "unknown"),
                      "os_version_full_version": host.get("os", {}).get("version", {}).get("full_version", "unknown"),
                      "os_version_major": host.get("os", {}).get("version", {}).get("major", "unknown"),
                      "os_version_minor": host.get("os", {}).get("version", {}).get("minor", "unknown"),
                      "version_build": host.get("version", {}).get('build', 'unknown'),
                      "version_full_version": host.get("version", {}).get('full_version', 'unknown'),
                      "version_major": host.get("version", {}).get('major', 'unknown'),
                      "version_minor": host.get("version", {}).get('minor', 'unknown'),
                      "version_revision": host.get("version", {}).get('revision', 'unknown'),
                      "vgpu_placement": host.get('vgpu_placement', 'unknown'),
                      "cluster_id": host.get('cluster', {}).get('id', 'unknown')}

            labels = ", ".join(f'{k}="{v}"' for k, v in labels.items())
            lines.append("# HELP auto_numa_status The host auto non uniform memory access (NUMA) status: 0/1/2 - disable/enable/unknown (number).\n")
            lines.append("# TYPE auto_numa_status gauge\n")
            lines.append(f"auto_numa_status{{{labels}}} { {'disable': 0, 'enable': 1, 'unknown': 2}.get(host.get('auto_numa_status', 'unknown'))}\n")
            lines.append("# HELP cpu_speed Current CPU speed (MHz).\n")
            lines.append("# TYPE cpu_speed gauge\n")
            lines.append(f"cpu_speed{{{labels}}} {host.get('cpu', {}).get('speed', 0)}\n")
            lines.append("# HELP cpu_topology_cores Number of VM CPU cores (number).\n")
            lines.append("# TYPE cpu_topology_cores gauge\n")
            lines.append(f"cpu_topology_cores{{{labels}}} {host.get('cpu', {}).get('topology', {}).get('cores', 0)}\n")
            lines.append("# HELP cpu_topology_sockets Number of VM CPU sockets (number).\n")
            lines.append("# TYPE cpu_topology_sockets gauge\n")
            lines.append(f"cpu_topology_sockets{{{labels}}} {host.get('cpu', {}).get('topology', {}).get('sockets', 0)}\n")
            lines.append("# HELP cpu_topology_threads Number of VM CPU threads (number).\n")
            lines.append("# TYPE cpu_topology_threads gauge\n")
            lines.append(f"cpu_topology_threads{{{labels}}} {host.get('cpu', {}).get('topology', {}).get('threads', 0)}\n")
            lines.append("# HELP device_passthrough_enabled Specifies whether host device passthrough is enabled on this host (bool).\n")
            lines.append("# TYPE device_passthrough_enabled gauge\n")
            lines.append(f"device_passthrough_enabled{{{labels}}} {1 if host.get('device_passthrough', {}).get('enabled', 'false') == 'true' else 0}\n")
            lines.append("# HELP external_status The host external status: 0/1/2/3/4/5 - error/failure/info/ok/warning/unknown (number).\n")
            lines.append("# TYPE external_status gauge\n")
            lines.append(f"external_status{{{labels}}} { {'error': 0, 'failure': 1, 'info': 2, 'ok': 3, 'warning': 4, 'unknown': 5}.get(host.get('external_status', 'unknown'))}\n")
            lines.append("# HELP hardware_information_supported_rng_source_hwrng Obtains random data from the /dev/hwrng (usually specialized HW generator) device (bool).\n")
            lines.append("# TYPE hardware_information_supported_rng_source_hwrng gauge\n")
            lines.append(f"hardware_information_supported_rng_source_hwrng{{{labels}}} {1 if "hwrng" in host.get('hardware_information', {}).get('supported_rng_sources', {}).get('supported_rng_source', {}) else 0}\n")
            lines.append("# HELP hardware_information_supported_rng_source_random Obtains random data from the /dev/random device (bool).\n")
            lines.append("# TYPE hardware_information_supported_rng_source_random gauge\n")
            lines.append(f"hardware_information_supported_rng_source_random{{{labels}}} {1 if "random" in host.get('hardware_information', {}).get('supported_rng_sources', {}).get('supported_rng_source', {}) else 0}\n")
            lines.append("# HELP hardware_information_supported_rng_source_urandom Obtains random data from the /dev/urandom device (bool).\n")
            lines.append("# TYPE hardware_information_supported_rng_source_urandom gauge\n")
            lines.append(f"hardware_information_supported_rng_source_urandom{{{labels}}} {1 if "urandom" in host.get('hardware_information', {}).get('supported_rng_sources', {}).get('supported_rng_source', {}) else 0}\n")
            lines.append("# HELP kdump_status The host KDUMP status. KDUMP happens when the host kernel has crashed and it is now going through memory dumping: 0/1/2 - disable/enable/unknown (number).\n")
            lines.append("# TYPE kdump_status gauge\n")
            lines.append(f"kdump_status{{{labels}}} { {'disabled': 0, 'enabled': 1, 'unknown': 2}.get(host.get('kdump_status', 'unknown'))}\n")
            lines.append("# HELP ksm_enabled Kernel SamePage Merging (KSM) reduces references to memory pages from multiple identical pages to a single page reference (bool).\n")
            lines.append("# TYPE ksm_enabled gauge\n")
            lines.append(f"ksm_enabled{{{labels}}} {1 if host.get('ksm', {}).get('enabled', 'false') == 'true' else 0}\n")
            lines.append("# HELP max_scheduling_memory The max scheduling memory on this host (bytes).\n")
            lines.append("# TYPE max_scheduling_memory gauge\n")
            lines.append(f"max_scheduling_memory{{{labels}}} {host.get('max_scheduling_memory', 0)}\n")
            lines.append("# HELP memory The amount of physical memory on this host (bytes).\n")
            lines.append("# TYPE memory gauge\n")
            lines.append(f"memory{{{labels}}} {host.get('max_scheduling_memory', 0)}\n")
            lines.append("# HELP numa_supported Specifies whether non uniform memory access (NUMA) is supported on this host (bool).\n")
            lines.append("# TYPE numa_supported gauge\n")
            lines.append(f"numa_supported{{{labels}}} {1 if host.get('numa_supported', 'false') == 'true' else 0}\n")
            lines.append("# HELP port The host port (number).\n")
            lines.append("# TYPE port gauge\n")
            lines.append(f"port{{{labels}}} {host.get('port', 0)}\n")
            lines.append("# HELP power_management_automatic_pm_enabled Toggles the automated power control of the host in order to save energy (bool).\n")
            lines.append("# TYPE power_management_automatic_pm_enabled gauge\n")
            lines.append(f"power_management_automatic_pm_enabled{{{labels}}} {1 if host.get('power_management', {}).get('automatic_pm_enabled', 'false') == 'true' else 0}\n")
            lines.append("# HELP power_management_enabled Indicates whether power management configuration is enabled or disabled (bool).\n")
            lines.append("# TYPE power_management_enabled gauge\n")
            lines.append(f"power_management_enabled{{{labels}}} {1 if host.get('power_management', {}).get('enabled', 'false') == 'true' else 0}\n")
            lines.append("# HELP power_management_kdump_detection Toggles whether to determine if kdump is running on the host before it is shut down (bool).\n")
            lines.append("# TYPE power_management_kdump_detection gauge\n")
            lines.append(f"power_management_kdump_detection{{{labels}}} {1 if host.get('power_management', {}).get('kdump_detection', 'false') == 'true' else 0}\n")
            lines.append("# HELP power_management_pm_proxies_cluster The fence proxy is selected from the same cluster as the fenced host (bool).\n")
            lines.append("# TYPE power_management_pm_proxies_cluster gauge\n")
            lines.append(f"power_management_pm_proxies_cluster{{{labels}}} {1 if 'cluster' in host.get('power_management', {}).get('pm_proxies', {}) else 0}\n")
            lines.append("# HELP power_management_pm_proxies_dc The fence proxy is selected from the same data center as the fenced host (bool).\n")
            lines.append("# TYPE power_management_pm_proxies_dc gauge\n")
            lines.append(f"power_management_pm_proxies_dc{{{labels}}} {1 if 'dc' in host.get('power_management', {}).get('pm_proxies', {}) else 0}\n")
            lines.append("# HELP power_management_pm_proxies_other_dc The fence proxy is selected from a different data center than the fenced host (bool).\n")
            lines.append("# TYPE power_management_pm_proxies_other_dc gauge\n")
            lines.append(f"power_management_pm_proxies_other_dc{{{labels}}} {1 if 'other_dc' in host.get('power_management', {}).get('pm_proxies', {}) else 0}\n")
            lines.append("# HELP protocol The protocol that the engine uses to communicate with the host: 0/1/2 - stomp/xml/unknown (number).\n")
            lines.append("# TYPE protocol gauge\n")
            lines.append(f"protocol{{{labels}}} { {'stomp': 0, 'xml': 1, 'unknown': 2}.get(host.get('protocol', 'unknown'))}\n")
            lines.append("# HELP reinstallation_required Specifies whether the host should be reinstalled (bool).\n")
            lines.append("# TYPE reinstallation_required gauge\n")
            lines.append(f"reinstallation_required{{{labels}}} {1 if host.get('reinstallation_required', 'false') == 'true' else 0}\n")
            lines.append("# HELP se_linux_mode The host SElinux status: 0/1/2/3 - disabled/enforcing/permissive/unknown (number).\n")
            lines.append("# TYPE se_linux_mode gauge\n")
            lines.append(f"se_linux_mode{{{labels}}} { {'disabled': 0, 'enforcing': 1, 'permissive': 2, 'unknown': 3}.get(host.get('se_linux', {}).get('mode', 'unknown'))}\n")
            lines.append("# HELP spm_priority The host storage pool manager (SPM) priority (number).\n")
            lines.append("# TYPE spm_priority gauge\n")
            lines.append(f"spm_priority{{{labels}}} {host.get('spm', {}).get('priority', 0)}\n")
            lines.append("# HELP spm_status The host storage pool manager (SPM) status: 0/1/2/3 - contending/none/spm/unknown (number).\n")
            lines.append("# TYPE spm_status gauge\n")
            lines.append(f"spm_status{{{labels}}} { {'contending': 0, 'none': 1, 'spm': 2, 'unknown': 3}.get(host.get('spm', {}).get('status', 'unknown'))}\n")
            lines.append("# HELP ssh_port The host SSH port (number).\n")
            lines.append("# TYPE ssh_port gauge\n")
            lines.append(f"ssh_port{{{labels}}} {host.get('ssh', {}).get('port', 0)}\n")
            lines.append("# HELP status The host status: 0/1/2/3/4/5/6/7/8/9/10/11/12/13/14/15 - connecting/down/error/initializing/install_failed/installing/installing_os/kdumping/maintenance/non_operational/non_responsive/pending_approval/preparing_for_maintenance/reboot/unassigned/up (number).\n")
            lines.append("# TYPE status gauge\n")
            lines.append(f"status{{{labels}}} { {'connecting': 0, 'down': 1, 'error': 2, 'initializing': 3, 'install_failed': 4,
                                                 'installing': 5, 'installing_os': 6, 'kdumping': 7, 'maintenance': 8,
                                                 'non_operational': 9, 'non_responsive': 10, 'pending_approval': 11, 'preparing_for_maintenance': 12,
                                                 'reboot': 13, 'unassigned': 14, 'up': 15}.get(host.get('status', 'unknown'))}\n")
            lines.append("# HELP summary_active The number of virtual machines active on the host (number).\n")
            lines.append("# TYPE summary_active gauge\n")
            lines.append(f"summary_active{{{labels}}} {host.get('summary', {}).get('active', 0)}\n")
            lines.append("# HELP summary_migrating The number of virtual machines migrating to or from the host (number).\n")
            lines.append("# TYPE summary_migrating gauge\n")
            lines.append(f"summary_migrating{{{labels}}} {host.get('summary', {}).get('migrating', 0)}\n")
            lines.append("# HELP summary_total The number of virtual machines present on the host (number).\n")
            lines.append("# TYPE summary_total gauge\n")
            lines.append(f"summary_total{{{labels}}} {host.get('summary', {}).get('total', 0)}\n")
            lines.append("# HELP transparent_hugepages_enabled Transparent huge page support expands the size of memory pages beyond the standard 4 KiB limit (bool).\n")
            lines.append("# TYPE transparent_hugepages_enabled gauge\n")
            lines.append(f"transparent_hugepages_enabled{{{labels}}} {1 if host.get('transparent_hugepages', {}).get('enabled', 'false') == 'true' else 0}\n")
            lines.append("# HELP type Indicates if the host contains a full installation of the operating system or a scaled-down version intended only to host virtual machines: 0/1/2/3 - ovirt_node/rhel/rhev_h/unknown (number).\n")
            lines.append("# TYPE type gauge\n")
            lines.append(f"type{{{labels}}} { {'ovirt_node': 0, 'rhel': 1, 'rhev_h': 2, 'unknown': 3}.get(host.get('type', 'unknown'))}\n")
            lines.append("# HELP update_available Specifies whether there is an oVirt-related update on this host (bool).\n")
            lines.append("# TYPE update_available gauge\n")
            lines.append(f"update_available{{{labels}}} {1 if host.get('update_available', 'false') == 'true' else 0}\n")

            for item in host.get("nics", {}).get("host_nic", {}):
                labels_str_stats = (f'{labels}, bonding_ad_partner_mac_address="{item.get("bonding", {}).get("ad_partner_mac", {}).get('address', "unknown")}", '
                                    f'nic_mac="{item.get("mac", {}).get("address", "unknown")}", '
                                    f'nic_profile="{item.get("vnic_profile", {}).get("id", "unknown")}", '
                                    f'nic_name="{item.get("name", "unknown")}", '
                                    f'base_interface="{item.get("base_interface", "unknown")}", '
                                    f'nic_id="{item.get("id", "unknown")}", '
                                    f'mac_address="{item.get("mac", {}).get("address", "unknown")}", '
                                    f'ip_address="{item.get("ip", {}).get("address", "unknown")}", '
                                    f'ip_gateway="{item.get("ip", {}).get("gateway", "unknown")}", '
                                    f'ip_netmask="{item.get("ip", {}).get("netmask", "unknown")}", '
                                    f'ip_version="{item.get("ip", {}).get("version", "unknown")}", '
                                    f'ipv6_address="{item.get("ipv6", {}).get("address", "unknown")}", '
                                    f'ipv6_gateway="{item.get("ipv6", {}).get("gateway", "unknown")}", '
                                    f'ipv6_netmask="{item.get("ipv6", {}).get("netmask", "unknown")}", '
                                    f'ipv6_version="{item.get("ipv6", {}).get("version", "unknown")}", '
                                    f'vlan_id="{item.get("vlan", {}).get("id", 0)}"')

                lines.append(f"# HELP boot_protocol The IPv4 boot protocol configuration of the NIC: 0/1/2/3/4/5 - autoconf/dhcp/none/poly_dhcp_autoconf/static/unknown (number).\n")
                lines.append(f"# TYPE boot_protocol gauge\n")
                lines.append(f"boot_protocol{{{labels_str_stats}}} { {'autoconf': 0, 'dhcp': 1, 'none': 2, 'poly_dhcp_autoconf': 3, 'static': 4, 'unknown': 5}.get(item.get('boot_protocol', "unknown"))}\n")
                lines.append(f"# HELP ipv6_boot_protocol The IPv6 boot protocol configuration of the NIC: 0/1/2/3/4/5 - autoconf/dhcp/none/poly_dhcp_autoconf/static/unknown (number).\n")
                lines.append(f"# TYPE ipv6_boot_protocol gauge\n")
                lines.append(f"ipv6_boot_protocol{{{labels_str_stats}}} { {'autoconf': 0, 'dhcp': 1, 'none': 2, 'poly_dhcp_autoconf': 3, 'static': 4, 'unknown': 5}.get(item.get('ipv6_boot_protocol', "unknown"))}\n")
                lines.append(f"# HELP ad_aggregator_id The ad_aggregator_id property of a bond or bond slave, for bonds in mode 4 (number).\n")
                lines.append(f"# TYPE ad_aggregator_id gauge\n")
                lines.append(f"ad_aggregator_id{{{labels_str_stats}}} {item.get('ad_aggregator_id', 0)}\n")
                lines.append(f"# HELP bridged Defines the bridged network status (bool).\n")
                lines.append(f"# TYPE bridged gauge\n")
                lines.append(f"bridged{{{labels_str_stats}}} {1 if item.get('bridged', 'false') == 'true' else 0}\n")
                lines.append(f"# HELP custom_configuration Indicates whether the host network interface has non-default custom configuration parameters (bool).\n")
                lines.append(f"# TYPE custom_configuration gauge\n")
                lines.append(f"custom_configuration{{{labels_str_stats}}} {1 if item.get('custom_configuration', 'false') == 'true' else 0}\n")
                lines.append(f"# HELP mtu The maximum transmission unit for the interface (number).\n")
                lines.append(f"# TYPE mtu gauge\n")
                lines.append(f"mtu{{{labels_str_stats}}} {item.get('mtu', 0)}\n")
                lines.append(f"# HELP speed Current negotiated link speed of the host network interface in bits per seconds (bits_per_second).\n")
                lines.append(f"# TYPE speed gauge\n")
                lines.append(f"speed{{{labels_str_stats}}} {item.get('speed', 0)}\n")
                lines.append(f"# HELP ad_aggregator_id The ad_aggregator_id property of a bond or bond slave, for bonds in mode 4 (number).\n")
                lines.append(f"# TYPE ad_aggregator_id gauge\n")
                lines.append(f"ad_aggregator_id{{{labels_str_stats}}} {item.get('ad_aggregator_id', 0)}\n")
                lines.append(f"# HELP status Defines the bridged network status (bool).\n")
                lines.append(f"# TYPE status gauge\n")
                lines.append(f"status{{{labels_str_stats}}} {1 if item.get('status', 'down') == 'up' else 0}\n")
                lines.append(f"# HELP check_connectivity Indicates whether connectivity check is enabled for the host network interface (bool).\n")
                lines.append(f"# TYPE check_connectivity gauge\n")
                lines.append(f"check_connectivity{{{labels_str_stats}}} {1 if item.get('check_connectivity', 'false') == 'true' else 0}\n")

                for nic_item in item.get("bonding", {}).get("options", {}).get("option", {}):
                    lines.append(f"# HELP {nic_item.get('name', 'unknown').replace('.', '_')} {nic_item.get('type', 'No description')} (number).\n")
                    lines.append(f"# TYPE {nic_item.get('name', 'unknown').replace('.', '_')} gauge\n")
                    lines.append(f"{nic_item.get('name', 'unknown').replace('.', '_')}{{{labels_str_stats}}} {nic_item.get('value', 0)}\n")

                for nic_item in item.get("statistics", {}).get("statistic", {}):
                    lines.append(f"# HELP {nic_item.get('name', 'unknown').replace('.', '_')} {nic_item.get('description', 'unknown')} ({nic_item.get('unit', 'unknown')}).\n")
                    lines.append(f"# TYPE {nic_item.get('name', 'unknown').replace('.', '_')} {nic_item.get('kind', 'unknown')}\n")
                    lines.append(f"{nic_item.get('name', 'unknown').replace('.', '_')}{{{labels_str_stats}}} {nic_item.get('values', {}).get('value', {})[0].get('datum', 0)
                    if len(nic_item.get('values', {})) > 0
                    else 0}\n")

            for item in host.get("statistics", {}).get("statistic", {}):
                lines.append(f"# HELP {item.get('name', 'unknown').replace('.', '_')} {item.get('description', 'unknown')} ({item.get('unit', 'unknown')}).\n")
                lines.append(f"# TYPE {item.get('name', 'unknown').replace('.', '_')} {item.get('kind', 'unknown')}\n")
                lines.append(f"{item.get('name', 'unknown').replace('.', '_')}{{{labels}}} {item.get('values', {}).get('value', {})[0].get('datum', 0)
                    if len(item.get('values', {})) > 0
                    else 0}\n")

        return lines


async def get_datacenters_statistics(session, token):
    url = f"{VIRT_SCHEME}://{VIRT_URL}/ovirt-engine/api/datacenters?follow=mac_pool,qoss,quotas,quotas.quotastoragelimits,quotas.quotaclusterlimits"
    async with session.get(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                           ssl=False) as resp:
        datacenters_statistics = await resp.json()

        lines = []

        for datacenter in datacenters_statistics["data_center"]:
            labels = {"object_type": "data_center",
                      "storage_format": datacenter.get("storage_format", "unknown"),
                      "supported_versions_major": datacenter.get("supported_versions", {}).get("version", {})[0].get("major", "unknown"),
                      "supported_versions_minor": datacenter.get("supported_versions", {}).get("version", {})[0].get("minor", "unknown"),
                      "version_major": datacenter.get("version", {}).get("major", "unknown"),
                      "version_minor": datacenter.get("version", {}).get("minor", "unknown"),
                      "name": datacenter.get("name", "unknown"),
                      "id": datacenter.get("id", "unknown")}

            labels = ", ".join(f'{k}="{v}"' for k, v in labels.items())
            lines.append("# HELP local Indicates whether the data center uses local storage (1) or shared storage (0) (bool).\n")
            lines.append("# TYPE local gauge\n")
            lines.append(f"local{{{labels}}} {1 if datacenter.get('local', 'false') == 'true' else 0}\n")
            lines.append("# HELP quota_mode Indicates whether quota enforcement is enabled for this datacenter (bool).\n")
            lines.append("# TYPE quota_mode gauge\n")
            lines.append(f"quota_mode{{{labels}}} { {'audit': 0, 'disabled': 1, 'enabled': 2, 'unknown': 3}.get(datacenter.get('quota_mode', 'unknown'))}\n")
            lines.append("# HELP status Datacenter status: 0/1/2/3/4/5/6 - contend/maintenance/not_operational/problematic/uninitialized/up/unknown  (number).\n")
            lines.append("# TYPE status gauge\n")
            lines.append(f"status{{{labels}}} { {'contend': 0, 'maintenance': 1, 'not_operational': 2, 'problematic': 3, 'uninitialized': 4, 'up': 5, 'unknown': 6}.get(datacenter.get('status', 'unknown'))}\n")

            for item in datacenter.get("mac_pool", {}).get("ranges", {}).get("range", {}):
                labels_str_stats = (f'{labels}, mac_pool_name="{item.get("mac_pool", {}).get("name", "unknown")}", '
                                    f'mac_pool_description="{item.get("mac_pool", {}).get("description", "unknown")}", '
                                    f'mac_pool_id="{item.get("mac_pool", {}).get("id", "unknown")}", '
                                    f'mac_pool_range_from="{item.get("mac_pool", {}).get("ranges", {}).get("range", {}).get("from", "unknown")}", '
                                    f'mac_pool_range_to="{item.get("mac_pool", {}).get("ranges", {}).get("range", {}).get("to", "unknown")}"')

                lines.append("# HELP allow_duplicates Defines whether duplicate MAC addresses are permitted in the pool (number).\n")
                lines.append("# TYPE allow_duplicates gauge\n")
                lines.append(f"allow_duplicates{{{labels_str_stats}}} {1 if datacenter.get('mac_pool', {}).get('allow_duplicates', 'false') == 'true' else 0}\n")
                lines.append("# HELP default_pool Defines whether this is the default pool (number).\n")
                lines.append("# TYPE default_pool gauge\n")
                lines.append(f"default_pool{{{labels_str_stats}}} {1 if datacenter.get('mac_pool', {}).get('default_pool', 'false') == 'true' else 0}\n")

            for item in datacenter.get("qoss", {}).get("qos", {}):
                labels_str_stats = (f'{labels}, qos_type="{item.get("type", "unknown")}", '
                                    f'qos_name="{item.get("name", "unknown")}", '
                                    f'qos_id="{item.get("id", "unknown")}"')

                lines.append("# HELP qos_max_read_iops Maximum permitted number of input operations per second (number).\n")
                lines.append("# TYPE qos_max_read_iops gauge\n")
                lines.append(f"qos_max_read_iops{{{labels_str_stats}}} {item.get('max_read_iops', 0)}\n")
                lines.append("# HELP qos_max_read_throughput Maximum permitted throughput for read operations (number).\n")
                lines.append("# TYPE qos_max_read_throughput gauge\n")
                lines.append(f"qos_max_read_throughput{{{labels_str_stats}}} {item.get('max_read_throughput', 0)}\n")
                lines.append("# HELP qos_max_write_iops Maximum permitted number of output operations per second (number).\n")
                lines.append("# TYPE qos_max_write_iops gauge\n")
                lines.append(f"qos_max_write_iops{{{labels_str_stats}}} {item.get('max_write_iops', 0)}\n")
                lines.append("# HELP qos_max_write_throughput Maximum permitted throughput for write operations (number).\n")
                lines.append("# TYPE qos_max_write_throughput gauge\n")
                lines.append(f"qos_max_write_throughput{{{labels_str_stats}}} {item.get('max_write_throughput', 0)}\n")

            for item in datacenter.get("quotas", {}).get("quota", {}):
                labels_str_stats = (f'{labels}, quota_name="{item.get("name", "unknown")}", '
                                    f'quota_description="{item.get("description", "unknown")}", '
                                    f'quota_id="{item.get("id", "unknown")}"')

                lines.append("# HELP cluster_hard_limit_pct Hard resource overcommit limit for the cluster, expressed as a percentage of physical capacity (number).\n")
                lines.append("# TYPE cluster_hard_limit_pct gauge\n")
                lines.append(f"cluster_hard_limit_pct{{{labels_str_stats}}} {item.get('cluster_hard_limit_pct', 0)}\n")
                lines.append("# HELP cluster_soft_limit_pct Soft resource overcommit limit for the cluster, expressed as a percentage of physical capacity (number).\n")
                lines.append("# TYPE cluster_soft_limit_pct gauge\n")
                lines.append(f"cluster_soft_limit_pct{{{labels_str_stats}}} {item.get('cluster_soft_limit_pct', 0)}\n")
                lines.append("# HELP storage_hard_limit_pct Hard storage usage limit for the datacenter, expressed as a percentage of total storage capacity (number).\n")
                lines.append("# TYPE storage_hard_limit_pct gauge\n")
                lines.append(f"storage_hard_limit_pct{{{labels_str_stats}}} {item.get('storage_hard_limit_pct', 0)}\n")
                lines.append("# HELP storage_soft_limit_pct Soft storage usage limit for the datacenter, expressed as a percentage of total storage capacity (number).\n")
                lines.append("# TYPE storage_soft_limit_pct gauge\n")
                lines.append(f"storage_soft_limit_pct{{{labels_str_stats}}} {item.get('storage_soft_limit_pct', 0)}\n")

                for quota_cluster_limit in item.get("quota_cluster_limits", {}).get("quota_cluster_limit", {}):
                    lines.append("# HELP memory_limit Memory limit for the quota at cluster level in bytes. A value of -1 indicates no limit (unlimited) (number).\n")
                    lines.append("# TYPE memory_limit gauge\n")
                    lines.append(f"memory_limit{{{labels_str_stats}}} {quota_cluster_limit.get('memory_limit', 0)}\n")
                    lines.append("# HELP memory_usage Current memory usage for the quota at cluster level in bytes (number).\n")
                    lines.append("# TYPE memory_usage gauge\n")
                    lines.append(f"memory_usage{{{labels_str_stats}}} {quota_cluster_limit.get('memory_usage', 0)}\n")
                    lines.append("# HELP vcpu_limit Virtual CPU limit for the quota at cluster level. A value of -1 indicates no limit (unlimited) (number).\n")
                    lines.append("# TYPE vcpu_limit gauge\n")
                    lines.append(f"vcpu_limit{{{labels_str_stats}}} {quota_cluster_limit.get('vcpu_limit', 0)}\n")
                    lines.append("# HELP vcpu_usage Number of virtual CPUs currently allocated under the quota at cluster level (number).\n")
                    lines.append("# TYPE vcpu_usage gauge\n")
                    lines.append(f"vcpu_usage{{{labels_str_stats}}} {quota_cluster_limit.get('vcpu_usage', 0)}\n")

                for quota_storage_limit in item.get("quota_storage_limits", {}).get("quota_storage_limit", {}):
                    lines.append("# HELP limit Storage capacity limit for the quota in bytes. A value of -1 indicates no limit (unlimited) (number).\n")
                    lines.append("# TYPE limit gauge\n")
                    lines.append(f"limit{{{labels_str_stats}}} {quota_storage_limit.get('limit', 0)}\n")
                    lines.append("# HELP usage Current storage usage for the quota in bytes (number).\n")
                    lines.append("# TYPE usage gauge\n")
                    lines.append(f"usage{{{labels_str_stats}}} {quota_storage_limit.get('usage', 0)}\n")

        return lines


async def get_clusters_statistics(session, token):
    url = f"{VIRT_SCHEME}://{VIRT_URL}/ovirt-engine/api/clusters?follow=enabledfeatures"
    async with session.get(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                           ssl=False) as resp:
        clusters_statistics = await resp.json()

        lines = []

        for cluster in clusters_statistics["cluster"]:
            labels = {"object_type": "cluster",
                      "bios_type": cluster.get("bios_type", "unknown"),
                      "cpu_architecture": cluster.get("cpu", {}).get("architecture", "unknown"),
                      "cpu_type": cluster.get("cpu", {}).get("type", "unknown"),
                      "migration_policy_id": cluster.get("migration", {}).get("policy", {}).get("id", "unknown"),
                      "version_major": cluster.get("version", {}).get("major", "unknown"),
                      "version_minor": cluster.get("version", {}).get("minor", "unknown"),
                      "name": cluster.get("name", "unknown"),
                      "id": cluster.get("id", "unknown")}

            labels = ", ".join(f'{k}="{v}"' for k, v in labels.items())

            for item in cluster.get("custom_scheduling_policy_properties", {}).get("property", {}):
                metric_name = "".join(f"_{c.lower()}" if c.isupper() else c for c in item.get('name', 'unknown'))[1:]
                lines.append(f"# HELP {metric_name.replace('.', '_')} (number).\n")
                lines.append(f"# TYPE {metric_name.replace('.', '_')} gauge\n")
                lines.append(f"{metric_name.replace('.', '_')}{{{labels}}} {item.get('value', 0)}\n")

            lines.append("# HELP error_handling_on_error Policy defining which virtual machines are migrated automatically when a cluster error or failure occurs: 0/1/2/3 - do_not_migrate/migrate/migrate_highly_available/unknown (number).\n")
            lines.append("# TYPE error_handling_on_error gauge\n")
            lines.append(f"error_handling_on_error{{{labels}}} { {'do_not_migrate': 0, 'migrate': 1, 'migrate_highly_available': 2, 'unknown': 3}.get(cluster.get('error_handling', {}).get('on_error', 'unknown'))}\n")
            lines.append("# HELP firewall_type The type of firewall to be used on hosts in this cluster: 0/1/2 - firewalld/iptables/unknown (number).\n")
            lines.append("# TYPE firewall_type gauge\n")
            lines.append(f"firewall_type{{{labels}}} { {'firewalld': 0, 'iptables': 1, 'unknown': 2}.get(cluster.get('firewall_type', 'unknown'))}\n")
            lines.append("# HELP ballooning_enabled Indicates whether memory ballooning is enabled for virtual machines in the cluster (bool).\n")
            lines.append("# TYPE ballooning_enabled gauge\n")
            lines.append(f"ballooning_enabled{{{labels}}} {1 if cluster.get('ballooning_enabled', 'false') == 'true' else 0}\n")
            lines.append("# HELP fencing_policy_enabled Enable or disable fencing on this cluster (bool).\n")
            lines.append("# TYPE fencing_policy_enabled gauge\n")
            lines.append(f"fencing_policy_enabled{{{labels}}} {1 if cluster.get('fencing_policy', {}).get('enabled', 'false') == 'true' else 0}\n")
            lines.append("# HELP fencing_policy_skip_if_connectivity_broken_enabled If enabled, we will not fence a host in case more than a configurable percentage of hosts in the cluster lost connectivity as well (bool).\n")
            lines.append("# TYPE fencing_policy_skip_if_connectivity_broken_enabled gauge\n")
            lines.append(f"fencing_policy_skip_if_connectivity_broken_enabled{{{labels}}} {1 if cluster.get('fencing_policy', {}).get('skip_if_connectivity_broken', {}).get('enabled', 'false') == 'true' else 0}\n")
            lines.append("# HELP fencing_policy_skip_if_connectivity_broken_threshold Threshold for connectivity testing (bool).\n")
            lines.append("# TYPE fencing_policy_skip_if_connectivity_broken_threshold gauge\n")
            lines.append(f"fencing_policy_skip_if_connectivity_broken_threshold{{{labels}}} {1 if cluster.get('fencing_policy', {}).get('skip_if_connectivity_broken', {}).get('threshold', 'false') == 'true' else 0}\n")
            lines.append("# HELP fencing_policy_skip_if_gluster_bricks_up A flag indicating if fencing should be skipped if Gluster bricks are up and running in the host being fenced (bool).\n")
            lines.append("# TYPE fencing_policy_skip_if_gluster_bricks_up gauge\n")
            lines.append(f"fencing_policy_skip_if_gluster_bricks_up{{{labels}}} {1 if cluster.get('fencing_policy', {}).get('skip_if_gluster_bricks_up', 'false') == 'true' else 0}\n")
            lines.append("# HELP fencing_policy_skip_if_gluster_quorum_not_met A flag indicating if fencing should be skipped if Gluster bricks are up and running and Gluster quorum will not be met without those bricks (bool).\n")
            lines.append("# TYPE fencing_policy_skip_if_gluster_quorum_not_met gauge\n")
            lines.append(f"fencing_policy_skip_if_gluster_quorum_not_met{{{labels}}} {1 if cluster.get('fencing_policy', {}).get('skip_if_gluster_quorum_not_met', 'false') == 'true' else 0}\n")
            lines.append("# HELP fencing_policy_skip_if_sd_active_enabled If enabled, we will skip fencing in case the host maintains its lease in the storage (bool).\n")
            lines.append("# TYPE fencing_policy_skip_if_sd_active_enabled gauge\n")
            lines.append(f"fencing_policy_skip_if_sd_active_enabled{{{labels}}} {1 if cluster.get('fencing_policy', {}).get('skip_if_sd_active', {}).get('enabled', 'false') == 'true' else 0}\n")
            lines.append("# HELP fips_mode FIPS mode of the cluster (bool).\n")
            lines.append("# TYPE fips_mode gauge\n")
            lines.append(f"fips_mode{{{labels}}} {1 if cluster.get('fips_mode', 'disabled') == 'enabled' else 0}\n")
            lines.append("# HELP gluster_service Indicates whether GlusterFS service is enabled for the cluster (bool).\n")
            lines.append("# TYPE gluster_service gauge\n")
            lines.append(f"gluster_service{{{labels}}} {1 if cluster.get('gluster_service', 'false') == 'true' else 0}\n")
            lines.append("# HELP ha_reservation Indicates whether resource reservation for high availability is enabled for the cluster (bool).\n")
            lines.append("# TYPE ha_reservation gauge\n")
            lines.append(f"ha_reservation{{{labels}}} {1 if cluster.get('ha_reservation', 'false') == 'true' else 0}\n")
            lines.append("# HELP ksm_enabled Indicates whether Kernel Samepage Merging (KSM) is enabled for the cluster (bool).\n")
            lines.append("# TYPE ksm_enabled gauge\n")
            lines.append(f"ksm_enabled{{{labels}}} {1 if cluster.get('ksm', {}).get('enabled', 'false') == 'true' else 0}\n")
            lines.append("# HELP ksm_merge_across_nodes Indicates whether Kernel Samepage Merging (KSM) is allowed to merge identical memory pages across NUMA nodes in the cluster (bool).\n")
            lines.append("# TYPE ksm_merge_across_nodes gauge\n")
            lines.append(f"ksm_merge_across_nodes{{{labels}}} {1 if cluster.get('ksm', {}).get('merge_across_nodes', 'false') == 'true' else 0}\n")
            lines.append("# HELP log_max_memory_used_threshold The memory consumption threshold for logging audit log events (number).\n")
            lines.append("# TYPE log_max_memory_used_threshold gauge\n")
            lines.append(f"log_max_memory_used_threshold{{{labels}}} {cluster.get('log_max_memory_used_threshold', 0)}\n")
            lines.append("# HELP log_max_memory_used_threshold_type The memory consumption threshold type for logging audit log events (1 if percentage, 0 if absolute_value_in_mb) (bool).\n")
            lines.append("# TYPE log_max_memory_used_threshold_type gauge\n")
            lines.append(f"log_max_memory_used_threshold_type{{{labels}}} {1 if cluster.get('log_max_memory_used_threshold_type', 'absolute_value_in_mb') == 'percentage' else 0}\n")
            lines.append("# HELP memory_policy_over_commit_percent Allowed memory overcommit for the cluster, expressed as a percantage of physical memory (number).\n")
            lines.append("# TYPE memory_policy_over_commit_percent gauge\n")
            lines.append(f"memory_policy_over_commit_percent{{{labels}}} {cluster.get('memory_policy', {}).get('over_commit', {}).get('percent', 0)}\n")
            lines.append("# HELP memory_policy_transparent_hugepages_enabled Allowed memory overcommit for the cluster, expressed as a percantage of physical memory (number).\n")
            lines.append("# TYPE memory_policy_transparent_hugepages_enabled gauge\n")
            lines.append(f"memory_policy_transparent_hugepages_enabled{{{labels}}} {1 if cluster.get('memory_policy', {}).get('transparent_hugepages', {}).get('enabled', 'false') == 'true' else 0}\n")
            lines.append("# HELP migration_auto_converge Migration network selection mode: 0/1/2/3 - false/true/inherit/unknown (number).\n")
            lines.append("# TYPE migration_auto_converge gauge\n")
            lines.append(f"migration_auto_converge{{{labels}}} { {'false': 0, 'true': 1, 'inherit': 2, 'unknown': 3}.get(cluster.get('migration', {}).get('auto_converge', 'unknown'))}\n")
            lines.append("# HELP migration_bandwidth_assignment_method Defines how the migration bandwidth is assigned: 0/1/2/3 - auto/custom/hypervisor_default/unknown (number).\n")
            lines.append("# TYPE migration_bandwidth_assignment_method gauge\n")
            lines.append(f"migration_bandwidth_assignment_method{{{labels}}} { {'auto': 0, 'custom': 1, 'hypervisor_default': 2, 'unknown': 3}.get(cluster.get('migration', {}).get('bandwidth', {}).get('assignment_method', 'unknown'))}\n")
            lines.append("# HELP migration_compressed Indicates whether memory compression is enabled for live migration: 0/1/2/3 - false/true/inherit/unknown (number).\n")
            lines.append("# TYPE migration_compressed gauge\n")
            lines.append(f"migration_compressed{{{labels}}} { {'false': 0, 'true': 1, 'inherit': 2, 'unknown': 3}.get(cluster.get('migration', {}).get('compressed', 'unknown'))}\n")
            lines.append("# HELP migration_encrypted Specifies whether the migration should be encrypted or not: 0/1/2/3 - false/true/inherit/unknown (number).\n")
            lines.append("# TYPE migration_encrypted gauge\n")
            lines.append(f"migration_encrypted{{{labels}}} { {'false': 0, 'true': 1, 'inherit': 2, 'unknown': 3}.get(cluster.get('migration', {}).get('encrypted', 'unknown'))}\n")
            lines.append("# HELP required_rng_source Representing the random generator backend types: 0/1/2/3 - hwrng/random/urandom/unknown (number).\n")
            lines.append("# TYPE required_rng_source gauge\n")
            lines.append(f"required_rng_source{{{labels}}} { {'hwrng': 0, 'random': 1, 'urandom': 2, 'unknown': 3}.get(cluster.get('required_rng_sources', {}).get('required_rng_source', 'unknown')[0])}\n")
            lines.append("# HELP switch_type The type of switch to be used by all networks in given cluster: 0/1/2 - legacy/ovs/unknown (number).\n")
            lines.append("# TYPE switch_type gauge\n")
            lines.append(f"switch_type{{{labels}}} { {'legacy': 0, 'ovs': 1, 'unknown': 2}.get(cluster.get('switch_type', 'unknown'))}\n")
            lines.append("# HELP threads_as_cores Indicates whether CPU threads (SMT/Hyper-Threading) are treated as separate CPU cores by the scheduler (bool).\n")
            lines.append("# TYPE threads_as_cores gauge\n")
            lines.append(f"threads_as_cores{{{labels}}} {1 if cluster.get('threads_as_cores', 'false') == 'true' else 0}\n")
            lines.append("# HELP trusted_service Indicates whether trusted services are enabled for virtual machine in the cluster (bool).\n")
            lines.append("# TYPE trusted_service gauge\n")
            lines.append(f"trusted_service{{{labels}}} {1 if cluster.get('trusted_service', 'false') == 'true' else 0}\n")
            lines.append("# HELP tunnel_migration Indicates whether virtual machine migration is performed via the management network tunnel (bool).\n")
            lines.append("# TYPE tunnel_migration gauge\n")
            lines.append(f"tunnel_migration{{{labels}}} {1 if cluster.get('tunnel_migration', 'false') == 'true' else 0}\n")
            lines.append("# HELP virt_service Indicates whether virtualization services are enabled for the cluster (bool).\n")
            lines.append("# TYPE virt_service gauge\n")
            lines.append(f"virt_service{{{labels}}} {1 if cluster.get('virt_service', 'false') == 'true' else 0}\n")
            lines.append("# HELP vnc_encryption Indicates whether VNC console connections to virtual machines are encrypted (bool).\n")
            lines.append("# TYPE vnc_encryption gauge\n")
            lines.append(f"vnc_encryption{{{labels}}} {1 if cluster.get('vnc_encryption', 'false') == 'true' else 0}\n")

        return lines


async def get_storagedomains_statistics(session, token):
    url = f"{VIRT_SCHEME}://{VIRT_URL}/ovirt-engine/api/storagedomains"
    async with session.get(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"}, ssl=False) as resp:
        storagedomains_statistics = await resp.json()

        lines = []

        for storagedomain in storagedomains_statistics["storage_domain"]:
            labels = {"object_type": "storagedomain",
                      "storage_type": storagedomain.get("storage", {}).get("type", "unknown"),
                      "name": storagedomain.get("name", "unknown"),
                      "id": storagedomain.get("id", "unknown")}

            labels = ", ".join(f'{k}="{v}"' for k, v in labels.items())

            lines.append("# HELP Amount of free storage space avalible in the storage domain (bytes).\n")
            lines.append("# TYPE available gauge\n")
            lines.append(f"available{{{labels}}} {storagedomain.get('available', 0)}\n")
            lines.append("# HELP This attribute indicates whether a data storage domain is used as backup domain or not (bool).\n")
            lines.append("# TYPE backup gauge\n")
            lines.append(f"backup{{{labels}}} {1 if storagedomain.get('backup', 'false') == 'true' else 0}\n")
            lines.append("# HELP Specifies block size in bytes for a storage domain (bytes).\n")
            lines.append("# TYPE block_size gauge\n")
            lines.append(f"block_size{{{labels}}} {storagedomain.get('block_size', 0)}\n")
            lines.append("# HELP Total logical size of all virtual disks allocated on the storage domain (bytes).\n")
            lines.append("# TYPE committed gauge\n")
            lines.append(f"committed{{{labels}}} {storagedomain.get('committed', 0)}\n")
            lines.append("# HELP Free space threshold, in percent, below which operations are blocked on the storage domain (number).\n")
            lines.append("# TYPE critical_space_action_blocker gauge\n")
            lines.append(f"critical_space_action_blocker{{{labels}}} {storagedomain.get('critical_space_action_blocker', 0)}\n")
            lines.append("# HELP Indicates whether disks' blocks on block storage domains will be discarded right before they are deleted (bool).\n")
            lines.append("# TYPE discard_after_delete gauge\n")
            lines.append(f"discard_after_delete{{{labels}}} {1 if storagedomain.get('discard_after_delete', 'false') == 'true' else 0}\n")
            lines.append("# HELP External health status of the storage domain as reported by the storage backend: 0/1/2/3/4/5 - error/failure/info/ok/warning/unknown (number).\n")
            lines.append("# TYPE external_status gauge\n")
            lines.append(f"external_status{{{labels}}} { {'error': 0, 'failure': 1, 'info': 2, 'ok': 3, 'warning': 4, 'unknown': 5}.get(storagedomain.get('external_status', 'unknown'))}\n")
            lines.append("# HELP Indicates whether the storage domain is the master domain of the data center (bool).\n")
            lines.append("# TYPE master gauge\n")
            lines.append(f"master{{{labels}}} {1 if storagedomain.get('master', 'false') == 'true' else 0}\n")
            lines.append("# HELP Storage domain metadata format version (v1-v5): 0/1/2/3/4/5 - v1/v2/v3/v4/v5/unknown (number).\n")
            lines.append("# TYPE storage_format gauge\n")
            lines.append(f"storage_format{{{labels}}} { {'v1': 0, 'v2': 1, 'v3': 2, 'v4': 3, 'v5': 4, 'unknown': 5}.get(storagedomain.get('storage_format', 'unknown'))}\n")
            lines.append("# HELP Indicates whether a block storage domain supports discard operations (bool).\n")
            lines.append("# TYPE supports_discard gauge\n")
            lines.append(f"supports_discard{{{labels}}} {1 if storagedomain.get('supports_discard', 'false') == 'true' else 0}\n")
            lines.append("# HELP Indicates whether a block storage domain supports the property that discard zeroes the data (bool).\n")
            lines.append("# TYPE supports_discard_zeroes_data gauge\n")
            lines.append(f"supports_discard_zeroes_data{{{labels}}} {1 if storagedomain.get('supports_discard_zeroes_data', 'false') == 'true' else 0}\n")
            lines.append("# HELP Storage domain role: 0/1/2/3/4/5/6 - data/export/image/iso/managed_block_storage/volume/unknown (number).\n")
            lines.append("# TYPE type gauge\n")
            lines.append(f"type{{{labels}}} { {'data': 0, 'export': 1, 'image': 2, 'iso': 3, 'managed_block_storage': 4, 'volume': 5, 'unknown': 6}.get(storagedomain.get('type', 'unknown'))}\n")
            lines.append("# HELP Used storage space in the storage domain (bytes).\n")
            lines.append("# TYPE used gauge\n")
            lines.append(f"used{{{labels}}} {storagedomain.get('used', 0)}\n")
            lines.append("# HELP Warning threshold for low free space on the storage domain (percent).\n")
            lines.append("# TYPE warning_low_space_indicator gauge\n")
            lines.append(f"warning_low_space_indicator{{{labels}}} {storagedomain.get('warning_low_space_indicator', 0)}\n")
            lines.append("# HELP Serves as the default value of wipe_after_delete for disks on this storage domain (bool).\n")
            lines.append("# TYPE wipe_after_delete gauge\n")
            lines.append(f"wipe_after_delete{{{labels}}} {1 if storagedomain.get('wipe_after_delete', 'false') == 'true' else 0}\n")

            for item in storagedomain.get("storage", {}).get("volume_group", {}).get("logical_units", {}).get("logical_unit", {}):
                labels_str_stats = (f'{labels}, logical_unit_product_id="{item.get("product_id", "unknown")}", '
                                    f'logical_unit_serial="{item.get("serial", "unknown")}", '
                                    f'logical_unit_address="{item.get("address", "unknown")}", '
                                    f'logical_unit_portal="{item.get("portal", "unknown")}", '
                                    f'logical_unit_target="{item.get("target", "unknown")}", '
                                    f'logical_unit_vendor_id="{item.get("vendor_id", "unknown")}", '
                                    f'logical_unit_volume_group_id="{item.get("volume_group_id", "unknown")}", '
                                    f'logical_unit_id="{item.get("id", "unknown")}",'
                                    f'logical_unit_lun_mapping="{item.get("lun_mapping", "unknown")}"')

                lines.append("# HELP The maximum number of bytes that can be discarded by the logical unit’s underlying storage in a single operation (bytes).\n")
                lines.append("# TYPE discard_max_size gauge\n")
                lines.append(f"discard_max_size{{{labels_str_stats}}} {item.get('discard_max_size', 0)}\n")
                lines.append("# HELP True, if previously discarded blocks in the logical unit’s underlying storage are read back as zeros (bool).\n")
                lines.append("# TYPE discard_zeroes_data gauge\n")
                lines.append(f"discard_zeroes_data{{{labels_str_stats}}} {1 if item.get('discard_zeroes_data', 'false') == 'true' else 0}\n")
                lines.append("# HELP Number of active multipath to the logical unit (LUN) (number).\n")
                lines.append("# TYPE paths gauge\n")
                lines.append(f"paths{{{labels_str_stats}}} {item.get('paths', 0)}\n")
                lines.append("# HELP Network port used to access the logical unit (LUN). For example iSCSI target port (number).\n")
                lines.append("# TYPE port gauge\n")
                lines.append(f"port{{{labels_str_stats}}} {item.get('port', 0)}\n")
                lines.append("# HELP Size of the logical unit (LUN) (bytes).\n")
                lines.append("# TYPE size gauge\n")
                lines.append(f"size{{{labels_str_stats}}} {item.get('size', 0)}\n")

            for item in storagedomain.get("data_centers", {}).get("data_center", {}):
                labels_str_stats = f'{labels}, data_center_id="{item.get("id", "unknown")}"'

                lines.append("# HELP Size of the logical unit (LUN) (bytes).\n")
                lines.append("# TYPE data_center_id gauge\n")
                lines.append(f"data_center_id{{{labels_str_stats}}} 1\n")

        return lines


async def gather_statistic():
    async with aiohttp.ClientSession() as session:
        token = await get_token(session)

        tasks = [get_vm_statistics(session, token),
                 get_hosts_statistics(session, token),
                 get_clusters_statistics(session, token),
                 get_datacenters_statistics(session, token),
                 get_storagedomains_statistics(session, token)]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        output = "".join(line for result in results
                                 if not isinstance(result, Exception)
                                 for line in result)

        return output


async def metrics_updater():
    global METRICS_CACHE

    while True:
        start = time.time()
        new_metrics = None

        try:
            log.info("Collecting metrics...")
            new_metrics = await gather_statistic()
        except Exception as e:
            log.exception(f"Metrics update failed: {e}")

        if new_metrics:
            METRICS_CACHE["data"] = new_metrics
            METRICS_CACHE["timestamp"] = time.time()

            duration = time.time() - start
            log.info(f"Metrics updated in {duration:.2f}s")
        else:
            log.warning("Keeping previous metrics cache")

        duration = int(time.time() - start)
        sleep_time = max(0, CACHE_TTL - duration)

        await asyncio.sleep(sleep_time)


@app.on_event("startup")
async def startup_event():
    log.info("Starting metrics background updater...")
    asyncio.create_task(metrics_updater())


@app.get("/metrics")
async def metrics():
    data = METRICS_CACHE.get("data")
    if not data:
        return Response(content="# HELP zvirt_exporter_not_ready Exporter cache is not ready\n"
                                "# TYPE zvirt_exporter_not_ready gauge\n"
                                "zvirt_exporter_not_ready 1\n",
                        status_code=200,
                        media_type="text/plain")

    return Response(content=f"{data}# HELP zvirt_exporter_not_ready Exporter cache is not ready\n"
                            f"# TYPE zvirt_exporter_not_ready gauge\n"
                            f"zvirt_exporter_not_ready 0",
                    media_type="text/plain")
