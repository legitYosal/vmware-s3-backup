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

## How this works?
For incremental backup, we will query vmware for the changed areas on a disk(with the latest change id we have from last incremental backup), it will respond with the offset and the length of changed area, so kaboom using Nbdkit and VDDK plugin we will read the exact length of bytes from that offset, Now we have the s3 dilemma, first I was keeping a single file on s3, for a disk, and then multi part uploading the changed areas and multi part copying the not changed areas, which had multiple problems:
1. First of all if a changed area is less than 5MB I am forced to do a over-read and read not changed areas
2. If a not changed area is less than 5 MB, I can not use multi part copy and I am forced to append it to a changed area hence over-read
3. I have no control over compression to S3, because it is a single file and I have different dimension of areas hence no compression

So our ultimate solution to save a backup on s3 will work like this:
1. First we will naturally do a full copy of disk
   1. we will read a 64MB chunk of the file
   2. if this chunk is all zero:
      1. save an empty file at vm-UUID/disk-UUID/full/chunk1 with metadata -> chunk1, sparse, 0(offset), 64MB(length)
   3. gzip the 64MB chunk and 
      1. save the gziped file at vm-UUID/disk-UUID/full/chunk2 with metadata -> chunk2, gzip, 64MB(offset), 64MB(length)
So when want to construct the original disk we do it like this:
1. find vm-UUID/disk-UUID/full/*
2. we can save number of files or we can list all files in vm-UUID/disk-UUID/full/*
3. for each chunk do this:
   1. if chunk is sparse:
      1. from offset to length create zero bytes and write to disk
   2. ungzip the chunk, from offset to length write to file 

Also for incremental backup we are not going to alter the original file, we are only going to save completely new files for example like this:
```
vm-UUID/disk-UUID/full/*
vm-UUID/disk-UUID/incremental-<DateTime1>/*
vm-UUID/disk-UUID/incremental-<DateTime2>/*
vm-UUID/disk-UUID/incremental-<DateTime3>/*
```
How to take an incremental backup?
1. we will query the changed areas from vmware
2. it will respond for example 10 sectors or more
3. we will append all of them on top of each other with a manifest header
4. gzip them together
5. if it is less than 5MB we will add a zero byte padding
6. upload to `vm-UUID/disk-UUID/incremental-<DateTime1>/*`
