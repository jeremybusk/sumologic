# README

This is used for missing minutes/hours/days reports as well as aggregating compressed files to different compression format(lz4, gzip, zstd) or time files (hours or days instead of minutes).


## Example 

This command will take messages stored in minutes files and aggregate to days files

```
./sumo-archive-tool --log-level debug aggregate --input-dir tmpin --output-dir tmpout --aggregate-from minute --aggregate-to day --output-compression gzip --input-compression gzip
```
