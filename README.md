# hadoop-metrics

hadoop-metrics is a wrapper for Hadoop Metrics API.

## Installation

### gem

```
gem install hadoop-metrics
```

## Usage

### Setup

Without `jmx` method, getting metrics depends on /metrics API.
So if you use this library, then you should enable dumping each metrics via hadoop-metrics.properties.

```
# example
mapred.class=org.apache.hadoop.metrics.spi.NoEmitMetricsContext
mapred.period=30 
```

### JobTracker

```
require 'hadoop_metrics/job_tracker'

jt = HadoopMetrics::JobTracker.new('localhost', 50030)
puts JSON.pretty_generate(jt.fairscheduler_jobs)
```

### TaskTracker

```
require 'hadoop_metrics/job_tracker'

tt = HadoopMetrics::TaskTracker.new('localhost', 50060)
puts JSON.pretty_generate(tt.shuffle_output)
```

### NOTE

NameNode and DataNode are not supported yet.

## Copyright

<table>
  <tr>
    <td>Author</td><td>Masahiro Nakagawa <repeatedly@gmail.com></td>
  </tr>
  <tr>
    <td>Copyright</td><td>Copyright (c) 2013- Masahiro Nakagawa</td>
  </tr>
  <tr>
    <td>License</td><td>MIT License</td>
  </tr>
</table>
