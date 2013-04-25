require 'hadoop_metrics/job_tracker'
require 'hadoop_metrics/task_tracker'

jt = HadoopMetrics::JobTracker.new('localhost', 50030, :snake_case => false)
#puts JSON.pretty_generate(jt.fairscheduler_jobs)
#puts JSON.pretty_generate(jt.mapred_jobtracker('hostName'))
#puts JSON.pretty_generate(jt.fairscheduler_pools)
puts JSON.pretty_generate(jt.fairscheduler_running_tasks)
#puts JSON.pretty_generate(jt.jmx)

#tt = HadoopMetrics::TaskTracker.new('localhost', 50060, :snake_case => false)
#puts JSON.pretty_generate(tt.shuffle_output)
#puts JSON.pretty_generate(tt.mapred)
