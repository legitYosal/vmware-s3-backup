# VMware s3 backup

This will enable you to fully backup incrementally an instance on the vmware vcenter/esxi to s3 directly, without any separate steps to convert and send it.

This repo is mostly stolen from [migratekit](https://github.com/vexxhost/migratekit) and I am trying to upgrade it to use S3 client. Because it will create so much change I have decided to make it to another repository.  

## Installing VDDK

In order to use command line tool, or use this repo as a package, for the end product you must download and configure the VDDK package.

1. Download VMware Virtual Disk Development Kit (VDDK) 8.0.2 for Linux
2. Once you've downloaded the file, you will need to extract the contents of the tarball to `/usr/lib64/vmware-vix-disklib/` on your system.

So for example you can see them on:

## CLI usage

In order to use the CLI provided by this repo you must:
1. Have a vmware either vCenter or vSphere
2. Have S3 Credentials
3. Have installed VDDK in previous step

### List vms:
You can list vms:
```
$ go run main.go list vms --detailed
┌─────────────────┬────┬───────────────────────────────────┬───────────┬───────────┬───────┬────────────────────┬───────────┬─────────────┐
│      NAME       │ ID │               PATH                │  STATUS   │ MEMORY GB │ CP US │       DISKS        │ SNAPSHOTS │ CBT ENABLED │
├─────────────────┼────┼───────────────────────────────────┼───────────┼───────────┼───────┼────────────────────┼───────────┼─────────────┤
│ Debian-Router   │ 2  │ /ha-datacenter/vm/Debian-Router   │ poweredOn │ 2         │ 2     │ [{Hard-disk-1 16}] │ 0         │ true        │
│ Debian-Target02 │ 4  │ /ha-datacenter/vm/Debian-Target02 │ poweredOn │ 2         │ 2     │ [{Hard-disk-1 20}] │ 0         │ false       │
│ Debian-Migrate  │ 5  │ /ha-datacenter/vm/Debian-Migrate  │ poweredOn │ 2         │ 2     │ [{Hard-disk-1 8}]  │ 0         │ false       │
└─────────────────┴────┴───────────────────────────────────┴───────────┴───────────┴───────┴────────────────────┴───────────┴─────────────┘
```
### Enable CBT:
```
$ go run main.go vm enable-cbt Debian-Target02
time=2025-10-13T15:48:19.525+03:30 level=INFO msg="CBT enabled successfully"
$ go run main.go list vms --detailed          
┌─────────────────┬────┬───────────────────────────────────┬───────────┬───────────┬───────┬────────────────────┬───────────┬─────────────┐
│      NAME       │ ID │               PATH                │  STATUS   │ MEMORY GB │ CP US │       DISKS        │ SNAPSHOTS │ CBT ENABLED │
├─────────────────┼────┼───────────────────────────────────┼───────────┼───────────┼───────┼────────────────────┼───────────┼─────────────┤
│ Debian-Router   │ 2  │ /ha-datacenter/vm/Debian-Router   │ poweredOn │ 2         │ 2     │ [{Hard-disk-1 16}] │ 0         │ true        │
│ Debian-Target02 │ 4  │ /ha-datacenter/vm/Debian-Target02 │ poweredOn │ 2         │ 2     │ [{Hard-disk-1 20}] │ 0         │ true        │
│ Debian-Migrate  │ 5  │ /ha-datacenter/vm/Debian-Migrate  │ poweredOn │ 2         │ 2     │ [{Hard-disk-1 8}]  │ 0         │ false       │
└─────────────────┴────┴───────────────────────────────────┴───────────┴───────────┴───────┴────────────────────┴───────────┴─────────────┘
```
