require 'hadoop_metrics/api'

module HadoopMetrics
  class JobTracker
    include API

    JSON_FILED_VALUES = %W(SummaryJson AliveNodesInfoJson BlacklistedNodesInfoJson QueueInfoJson)

    def info
      via_jmx('hadoop:service=JobTracker,name=JobTrackerInfo', JSON_FILED_VALUES).first
    end

    def fairscheduler_pools(column = 'name')
      group_by('fairscheduler', 'pools', column)
    end

    def fairscheduler_jobs(column = 'name')
      group_by('fairscheduler','jobs', column)
    end

    def fairscheduler_running_tasks(target = 'pools')
      fs = metrics['fairscheduler']
      return nil if fs.nil?

      targets = fs[target]
      return nil if targets.nil?

      each_tasks = {}
      targets.each { |target|
        name = target.first['name']
        each_tasks[name] ||= 0
        each_tasks[name] += target.last['runningTasks']
      }
      each_tasks
    end
  end
end
