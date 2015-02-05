---
title: 'Lab 1: Getting started with the QuickStart VM'
sidebar: hidden
layout: page
---

In this lab, you will get the QuickStart VM running on your laptop.

Most of you have already downloaded a copy of the VM, but there are thumb drives with images if anyone needs them.

**Please do not download the VM over the conference wireless!**

(If you're not at a conference right now, you can [download the VM from Cloudera][qsvm].)

[qsvm]: http://www.cloudera.com/content/cloudera/en/downloads/quickstart_vms/cdh-5-3-x.html

## Recommendations

### Do all your work inside the VM

In the time available, we can't troubleshoot port forwarding, shared folders, or copy and paste settings.

## Steps

### 1. Unpack and start the VM

Once the VM is running and you can see the desktop, you're ready to go.

Useful information:

* **What are the usernames/passwords for the VM?**
  * HUE: cloudera/cloudera
  * Cloudera manager: cloudera/cloudera
  * Login: cloudera/cloudera

* **How do I get my mouse back?**
  * If your mouse/keyboard is stuck in the VM (captured), you can usually
    release it by pressing the right `CTRL` key. If you don't have one (or that
    didn't work), then the release key will be in the **lower-right** of the
    VirtualBox window

Troubleshooting suggestions:

* **How do I fix "VTx" errors?**
  * Reboot your computer and enter BIOS
  * Find the "Virtualization" settings, usually under "Security" and _enable_
    all of the virtualization options

* **I can't find the file in VirtualBox (or VMWare)!**
  * You probably need to unpack it.

* **How do I unpack a `.7z` file?**
  * You can install 7zip in Windows, which can _extract_ the VM files from the `.7z`file.
  * For linux or mac, `cd` to where you copied the file and run `7zr e <name>.7z`
  * You should be able to import the extracted files to VirtualBox or VMWare

* **How do I open a `.ovf` file?**
  * Install and open [VirtualBox][vbox] on your computer
  * Under the menu "File", select "Import..."
  * Navigate to where you unpacked the `.ovf` file and select it

* **What is a `.vmdk` file?**
  * The `.vmdk` file is the virtual machine disk image that accompanies a
    `.ovf` file, which is a portable VM description.

* **How do I open a `.vbox` file?**
  * Install and open [VirtualBox][vbox] on your computer
  * Under the menu "Machine", select "Add..."
  * Navigate to where you unpacked the `.vbox` file and select it

* **Other problems**
  * Using VirtualBox? Try using VMWare.
  * Using VMWare? Try using VirtualBox.

[vbox]: https://www.virtualbox.org/wiki/Downloads

### 2. Explore Hue

While we make sure everyone has the VM running, try out [Hue][hue]!

[hue]: http://quickstart.cloudera:8888/about/

## Next

* Move on to the next lab: [Create a movies dataset][lab-2]

[lab-2]: 2-create-a-movies-dataset.html
