# Experian output plugin for Embulk

Upload records and reserve mail delivery with Experian.

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: yes

## Configuration

- **option1**: description (integer, required)
- **option2**: description (string, default: `"myvalue"`)
- **option3**: description (string, default: `null`)

## Example

```yaml
out:
  type: experian
  option1: example1
  option2: example2
```


## Build

```
$ rake
```
