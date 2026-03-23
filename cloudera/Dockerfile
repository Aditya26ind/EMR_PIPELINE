FROM cloudera/quickstart:latest

# Cloudera's official QuickStart image bundles CDH components (HDFS, YARN, Hive, Impala, HBase, Hue, etc.)
# and a helper entrypoint (/usr/bin/docker-quickstart) that initializes and launches the core services
# in a single-node, non-production setup. This Dockerfile simply re-tags that image so you can build
# and run it locally from this repo.

# Set a sane default hostname; many Hadoop configs expect it to be stable.
ENV HOSTNAME=quickstart.cloudera

# Expose common service ports used by the QuickStart image.
EXPOSE 8020 8022 8888 7180 7187 8889 8983 21050 10000 10020 8021 50070 50075 8023

# Launch the bundled init script that starts HDFS, YARN, Hive Metastore, Impala, Hue, etc.
CMD ["/usr/bin/docker-quickstart"]
