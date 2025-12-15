# Snakemake Storage Plugin for NERSC

A Snakemake plugin to efficiently interact with the NERSC filesystem. Current
supported features:

- Read a file or perform globbing on the read-only Community filesystem mount
  point at `/dvs_ro`
