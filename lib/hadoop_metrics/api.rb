# -*- coding: utf-8 -*-
require 'net/http'
require 'json'

module HadoopMetrics
  module API
    def initialize(host, port, opts = {})
      @endpoint = "#{host}:#{port}"
      @metrics_endpoint = URI("http://#{@endpoint}/metrics?format=json")
      @snake_case = opts.has_key?(:snake_case) ? opts[:snake_case] : true
      @name = opts[:name] || host
      @metrics_cache = nil
    end

    attr_reader :name

    def metrics(force = true)
      if !@metrics_cache.nil? and !force
        return @metrics_cache
      end

      @metrics_cache = HadoopMetrics.get_response(@metrics_endpoint)
      @metrics_cache
    end

    GCNameMap = { 
      'PS Scavenge' => 'minor',  # for backward compatibility
      'PS MarkSweep' => 'major', # for backward compatibility
      'ConcurrentMarkSweep' => 'c_mark_sweep',
      'ParNew' => 'par_new'
    }

    def gc
      disable_snake_case {
        result = query_jmx('java.lang:type=GarbageCollector,name=*').map { |jmx_gc_info|
          return nil if jmx_gc_info['LastGcInfo'].nil?

          gc_info = {'type' => GCNameMap[jmx_gc_info['Name']]}
          gc_info['estimated_time'] = jmx_gc_info['CollectionTime']
          gc_info['count'] = jmx_gc_info['CollectionCount']

          last_gc_info = jmx_gc_info['LastGcInfo']
          gc_info['last_start'] = last_gc_info['startTime']
          gc_info['last_duration'] = last_gc_info['duration']
          gc_info['after_gc'] = calc_memory_usage(last_gc_info)

          gc_info
        }
      }
    end

    MegaByte = 1024.0 * 1024

    def memory
      disable_snake_case {
        result = {}

        memory = query_jmx('java.lang:type=Memory').first
        heap, non_heap = memory['HeapMemoryUsage'], memory['NonHeapMemoryUsage']
        result['committed'] = (heap['committed'] + non_heap['committed']) / MegaByte
        result['used'] = (heap['used'] + non_heap['used']) / MegaByte
        result['max'] = (heap['max'] + non_heap['max']) / MegaByte

        # Can we use 'max' attribute instead of -Xmx option?
        arguments = get_jmx('java.lang:type=Runtime::InputArguments').first['InputArguments']
        result['mx_option'] = arguments.select { |arg| arg =~ /-Xmx(.*)m/ }.last["-Xmx".size..-2].to_i

        result
      }
    end

    def query_jmx(query, json_fields = [])
      via_jmx('qry', query, json_fields)
    end

    def get_jmx(query, json_fields = [])
      via_jmx('get', query, json_fields)
    end

    def via_jmx(type, query, json_fields = [])
      HadoopMetrics.get_response(URI("http://#{@endpoint}/jmx?#{type}=#{query}"))['beans'].map { |jmx_json|
        json_fields.each { |f|
          jmx_json[f] = JSON.parse(jmx_json[f])
        }
        if @snake_case
          jmx_json = HadoopMetrics.snake_cased(jmx_json)
        end

        jmx_json
      }
    end

    private

    def get_force(opts)
      opts.has_key?(:force) ? opts[:force] : true
    end

    def calc_memory_usage(gc_info)
      after_gc = {'committed' => 0, 'used' => 0, 'max' => 0}
      gc_info['memoryUsageAfterGc'].each { |area|
        usage = area['value']
        after_gc['committed'] += usage['committed']
        after_gc['used'] += usage['used']
        after_gc['max'] += usage['max']
      }
      after_gc

      # TODO: Add memoryUsageBeforeGc if needed
    end

    def group_by(category, target, column, force)
      categories = metrics(force)[category]
      return nil if categories.nil?

      targets = categories[target]
      return nil if targets.nil?

      targets.map { |target|
        HadoopMetrics.merge_data(target, @snake_case)
      }.group_by { |target|
        target[column]
      }
    end

    def disable_snake_case
      old_snake_case = @snake_case
      @snake_case = false

      yield
    ensure
      @snake_case = old_snake_case
    end

    def method_missing(method, *args)
      category, target = method.to_s.split('_', 2)
      group_by(category, target, *args)
    end
  end

  def self.get_response(endpoint)
    response = Net::HTTP.get_response(endpoint)
    if response.code.to_i == 200
      JSON.parse(response.body)
    else
      raise "Failed to get a response: code = #{response.code}, body = #{response.body}"
    end
  end

  def self.merge_data(data, snake_case)
    f = data.first
    f.merge!(data.last)
    snake_case ? snake_cased(f) : f
  end

  def self.to_snake_case(name)
    name[0] = name[0].chr.downcase
    name.gsub(/[A-Z]/) { |n| "_#{n.downcase}" }
  end

  def self.snake_cased(json)
    snake_cased_json = {}
    json.each_pair { |key, value|
      v = json[key]
      if v.is_a?(Hash)
        v = snake_cased(v)
      end
      snake_cased_json[HadoopMetrics.to_snake_case(key.dup)] = v
    }
    snake_cased_json
  end
end
