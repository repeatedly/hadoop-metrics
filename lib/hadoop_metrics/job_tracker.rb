require "hadoop_metrics/api"

module HadoopMetrics
  class JobTracker
    include API

    JSON_FILED_VALUES = %W(SummaryJson AliveNodesInfoJson BlacklistedNodesInfoJson QueueInfoJson)

    def info
      query_jmx('hadoop:service=JobTracker,name=JobTrackerInfo', JSON_FILED_VALUES).first
    end

    def mapred(opts = {})
      disable_snake_case {
        group_by('mapred', 'jobtracker', 'hostName', get_force(opts)).each_pair { |k, v|
          return v.first
        }
      }
    end

    def fairscheduler_pools(opts = {})
      group_by('fairscheduler', 'pools', get_column(opts), get_force(opts))
    end

    def fairscheduler_jobs(opts = {})
      group_by('fairscheduler','jobs', get_column(opts), get_force(opts))
    end

    def fairscheduler_running_tasks(opts = {})
      fs = metrics(get_force(opts))['fairscheduler']
      return nil if fs.nil?

      targets = fs[get_target(opts)]
      return nil if targets.nil?

      each_tasks = {}
      targets.each { |target|
        name = target.first['name']
        each_tasks[name] ||= 0
        each_tasks[name] += target.last['runningTasks']
      }
      each_tasks
    end

    private

    def get_column(opts)
      opts[:column] || 'name'
    end

    def get_target(opts)
      opts[:target] || 'pools'
    end
  end
end
