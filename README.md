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
### List existing backups:
```
$ go run main.go list backups
┌─────────────────────────┬─────────────────┬────────────────┬────────────────────────────────────────┐
│       OBJECT KEY        │     VM KEY      │     DISKS      │             ROOT DISK KEY              │
├─────────────────────────┼─────────────────┼────────────────┼────────────────────────────────────────┤
│ vm-data-Debian-Target02 │ Debian-Target02 │ [0xc000336000] │ vm-data-Debian-Target02/disk-data-2000 │
└─────────────────────────┴─────────────────┴────────────────┴────────────────────────────────────────┘
```
### Start a backup cycle
```
$ go run main.go start cycle Debian-Target02
```

### Download an existing backup to disk
```
$ go run main.go download-backup Debian-Target02 2000 ../disk200.raw
```
In order to verify it very fast if it has the correct data or not on ubuntu you can do these steps:
```
$ sudo mkdir /mnt/disk-data
$ LOOP_DEVICE=$(sudo losetup -f --show ~/disk2000.raw)
$ sudo kpartx -a $LOOP_DEVICE
$ sudo mount /dev/mapper/loop0p1 /mnt/disk-data
```
After verifying to un mount:
```
$ sudo umount /mnt/disk-data
$ sudo kpartx -d $LOOP_DEVICE
$ sudo losetup -d $LOOP_DEVICE
```

### Upload from local to VMware
You can upload a disk from local to VMware, also note that the exported disk data is in raw format and you have to convert it to vmdk first, next:
```
$ go run main.go restore-disk --data-store-name DS1 --local-path ./disk2000.vmdk --remote-path RestoredImages
```

## How this works?
For incremental backup, we will query vmware for the changed areas on a disk(with the latest change id we have from last incremental backup), it will respond with the offset and the length of changed area, so kaboom using Nbdkit and VDDK plugin we will read the exact length of bytes from that offset, Now we have the s3 dilemma, first I was keeping a single file on s3, for a disk, and then multi part uploading the changed areas and multi part copying the not changed areas, which had multiple problems:
1. First of all if a changed area is less than 5MB I am forced to do a over-read and read not changed areas
2. If a not changed area is less than 5 MB, I can not use multi part copy and I am forced to append it to a changed area hence over-read
3. I have no control over compression to S3, because it is a single file and I have different dimension of areas hence no compression

So our ultimate solution to save a backup on s3, we are going to ditch the multi part upload, and save each chunk in a new file for example: `vm-<key>/disk-<key>/full/00004` keeping the 64MB data in that file, enables us to compress it using zstd, also we will keep a manifest file to keep the state of the backup.
