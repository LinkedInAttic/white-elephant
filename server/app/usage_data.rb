# Copyright 2012 LinkedIn, Inc

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'app/cache_builder'
require 'app/executor'
require 'app/usage_database'
require 'app/cube'
require 'enumerator'
require 'tzinfo'
require 'date'
require 'time'
require 'set'
require 'yaml'
require 'tzinfo'

class UsageData

  @cache = CacheBuilder.create(UsageData)

  class << self

    def clear_cache
      cache.invalidate_all
    end

    def fetch_clusters
      cache_query_clusters
    end

    def fetch_users(cluster)
      cache_query_users(cluster)
    end

    def fetch_per_user_data(cluster,users,time,type)
      puts "Fetching per user data"

      where = {
        :cluster => {:equals => cluster}
      }

      where_for_type(where,type)

      measure = data_key_for_type(type)

      what = [:userName,:time]

      # when querying time this affects how it is aggregated
      time_unit = time[:unit]
      time_zone = time[:zone]

      cube = cache_query_by_time(what,where,measures_to_query,time_unit,time_zone)

      users_set = Set.new(users)

      cube = cube.filter_on(:userName) do |val|
        users_set.include?(val)
      end

      return_val = get_time_range(cluster,time,time_zone)

      default_value = measure_default_value(measure)

      return_val[:users] = []

      users.each do |user|
        user_data = {}

        user_cube = cube.slice(user)

        user_data[:user] = user
        user_data[:data] = return_val[:times].map do |t|
          values = user_cube && user_cube.cube[t]
          (values && values[measure]) || default_value
        end

        return_val[:users] << user_data
      end

      return_val
    end

    # TODO this is very similar to the previous method, figure out how to share code
    def fetch_aggregated_data(cluster,users,time,type)
      puts "Fetching aggregated data"

      where = {
        :cluster => {:equals => cluster}
      }

      where_for_type(where,type)

      measure = data_key_for_type(type)

      what = [:userName,:time]

      # when querying time this affects how it is aggregated
      time_unit = time[:unit]
      time_zone = time[:zone]

      cube = cache_query_by_time(what,where,measures_to_query,time_unit,time_zone)

      users_set = Set.new(users)

      cube = cube.filter_on(:userName) do |val|
        users_set.include?(val)
      end

      cube = cube.collapse_on(:userName)

      return_val = get_time_range(cluster,time,time_zone)
      return_val[:users] = users.join(",")

      default_value = measure_default_value(measure)

      # track the earliest and latest times with data in order to build a range
      start_time = nil
      end_time = nil

      return_val[:data] = return_val[:times].map do |t|
        values = cube && cube.cube[t]
        (values && values[measure]) || default_value
      end

      return_val[:time_range] = [start_time,end_time]

      return_val
    end

    def measures_to_query
      [:elapsedMinutes, :cpuMinutes, :started, :finished, :reduceShuffleBytes]
    end

    def where_for_type(where, type)
      case type

      when :minutesMap
        where[:type] = {:equals => :MAP}

      when :minutesReduce
        where[:type] = {:equals => :REDUCE}

      when :minutesExcessTotal
        where[:excess] = {:equals => true}

      when :minutesExcessMap
        where[:type] = {:equals => :MAP}
        where[:excess] = {:equals => true}

      when :minutesExcessReduce
        where[:type] = {:equals => :REDUCE}
        where[:excess] = {:equals => true}

      when :minutesSuccess
        where[:status] = {:equals => :SUCCESS}

      when :minutesKilled
        where[:status] = {:equals => :KILLED}

      when :minutesFailed
        where[:status] = {:equals => :FAILED}

      when :successStarted, :successFinished
        where[:status] = {:equals => :SUCCESS}

      when :failedStarted, :failedFinished
        where[:status] = {:equals => :FAILED}

      when :killedStarted,:killedFinished
        where[:status] = {:equals => :KILLED}

      when :mapStarted, :mapFinished
        where[:type] = {:equals => :MAP}

      when :reduceStarted, :reduceFinished
        where[:type] = {:equals => :REDUCE}

      when :reduceShuffleBytes
        where[:type] = {:equals => :REDUCE}

      end
    end

    def query_for_type(query,type)
      case type

      when :minutesMap
        query[:type] = :MAP

      when :minutesReduce
        query[:type] = :REDUCE

      when :minutesExcessTotal
        query[:excess] = true

      when :minutesExcessMap
        query[:type] = :MAP
        query[:excess] = true

      when :minutesExcessReduce
        query[:type] = :REDUCE
        query[:excess] = true

      when :minutesSuccess
        query[:status] = :SUCCESS

      when :minutesKilled
        query[:status] = :KILLED

      when :minutesFailed
        query[:status] = :FAILED

      when :successStarted, :successFinished
        query[:status] = :SUCCESS

      when :failedStarted, :failedFinished
        query[:status] = :FAILED

      when :killedStarted,:killedFinished
        query[:status] = :KILLED

      when :mapStarted, :mapFinished
        query[:type] = :MAP

      when :reduceStarted, :reduceFinished
        query[:type] = :REDUCE

      when :reduceShuffleBytes
        query[:type] = :REDUCE

      end
    end

    def data_key_for_type(type)
      case type

      when :minutesTotal, :minutesMap, :minutesReduce, :minutesExcessTotal, :minutesExcessMap, :minutesExcessReduce, :minutesSuccess, :minutesKilled, :minutesFailed
        :elapsedMinutes

      when :cpuTotal
        :cpuMinutes

      when :reduceShuffleBytes
        :reduceShuffleBytes

      when :successStarted, :failedStarted, :killedStarted, :totalStarted, :mapStarted, :reduceStarted
        :started

      when :successFinished, :failedFinished, :killedFinished, :totalFinished, :mapFinished, :reduceFinished
        :finished

      else
        raise "Unrecognized type: #{type}"

      end
    end

    def measure_default_value(measure)
      case measure
      when :elapsedMinutes, :cpuMinutes
        0.0
      when :started, :finished, :reduceShuffleBytes
        0
      else
        raise "Unrecognized type: #{measure}"
      end
    end

    def get_time_range_for_cluster(cluster)
      db_conn = UsageDatabase.connection
      statement = db_conn.create_statement 
      query_string = "SELECT MIN(time), MAX(time) FROM Usage where cluster='#{cluster}'"
      puts "Executing query"
      result = statement.execute_query(query_string)
      puts "Done!"
      if result.next
        [result.get_object(1).get_time,result.get_object(2).get_time]
      end
    end

    def get_time_range(cluster, time, time_zone)

      tz = TZInfo::Timezone.get(time_zone)

      return_val = {}

      min_time,max_time = get_time_range_for_cluster(cluster)

      # truncate start and end with min and max times from DB for cluster
      local_start_time = tz.utc_to_local(Time.at(time[:start]/1000).getutc)
      local_end_time = tz.utc_to_local(Time.at(time[:end]/1000).getutc)

      puts "Query start time: #{local_start_time}"
      puts "Query end time: #{local_end_time}"

      local_start_time = tz.utc_to_local(Time.at([time[:start],min_time].max/1000).getutc)
      local_end_time = tz.utc_to_local(Time.at([time[:end],max_time].min/1000).getutc)

      puts "Truncated start time: #{local_start_time}"
      puts "Truncated end time: #{local_end_time}"

      return_val[:times] = case time[:unit]

        when "HOURS"
          step_size_in_ms = 3600*1000
          # round to nearest hour, then convert to seconds
          range_start = ((time[:start]/(step_size_in_ms.to_f)).floor*step_size_in_ms).to_i
          range_end = ((time[:end]/(step_size_in_ms.to_f)).floor*step_size_in_ms).to_i

          times = []
          (range_start..range_end).step(step_size_in_ms).each do |time|
            times << time
          end
          times

        when "DAYS"
          start_date = Date.new(local_start_time.year,local_start_time.month,local_start_time.day)
          end_date = Date.new(local_end_time.year,local_end_time.month,local_end_time.day)

          start_date = start_date.next_day
          end_date = end_date.prev_day

          puts "Rounded start date: #{start_date}"
          puts "Rounded end date: #{end_date}"

          curr_date = start_date

          times = []
          while curr_date <= end_date do
            local_time = Time.local(curr_date.year,curr_date.month,curr_date.day)
            timestamp = tz.local_to_utc(local_time).to_i
            times << timestamp*1000

            curr_date = curr_date.next_day
          end

          times

        when "WEEKS"
          start_date = Date.new(local_start_time.year,local_start_time.month,local_start_time.day)
          end_date = Date.new(local_end_time.year,local_end_time.month,local_end_time.day)

          # round down to Sunday of the week each date belongs to
          start_date = start_date.prev_day(start_date.wday)
          end_date = end_date.prev_day(end_date.wday)

          start_date = start_date.next_day(7)
          end_date = end_date.prev_day(7)

          puts "start_date: #{start_date}"
          puts "end_date: #{end_date}"

          curr_date = start_date

          times = []
          while curr_date <= end_date do
            local_time = Time.local(curr_date.year,curr_date.month,curr_date.day)
            timestamp = tz.local_to_utc(local_time).to_i
            times << timestamp*1000

            curr_date = curr_date.next_day(7)
          end

          times

        when "MONTHS"
          start_date = Date.new(local_start_time.year,local_start_time.month)
          end_date = Date.new(local_end_time.year,local_end_time.month)

          start_date = start_date.next_month
          end_date = end_date.prev_month

          puts "start_date: #{start_date}"
          puts "end_date: #{end_date}"

          curr_date = start_date

          times = []
          while curr_date <= end_date do
            local_time = Time.local(curr_date.year,curr_date.month)
            timestamp = tz.local_to_utc(local_time).to_i
            times << timestamp*1000

            curr_date = curr_date.next_month
          end

          puts times.inspect

          times
        else
          raise "bad unit: #{time[:unit]}"
      end

      return_val
    end

    def method_missing(method,*args)
      # all the cache_* method calls are delegated to the cache,
      # which deserializes the method name and parameters and invokes
      # the method
      if method.to_s =~ /^cache_(.+)/
        cache.get(YAML::dump({
          :method => Regexp.last_match(1),
          :params => args
        }))
      else
        raise "Unrecognized method: #{method}"
      end
    end

    private 

    attr_accessor :cache

  end

  # cache requires a constructor for key
  def initialize(key)
    @key = key
  end

  # cache invokes call to get the value for the key
  def call
    @key = YAML::load(@key)
    puts "UsageLoader call for:\n#{@key.inspect}"
    self.send(@key[:method],*@key[:params])
  rescue => e
    puts e.inspect
    puts e.backtrace
  end

  private

  def query_clusters
    db_conn = UsageDatabase.connection

    puts "Fetching clusters"
    start = Time.now

    clusters = {}   

    statement = db_conn.create_statement
    result = statement.execute_query("SELECT DISTINCT cluster FROM Usage;")

    while result.next do
      cluster_name = result.get_string(1)
      clusters[cluster_name] = true
    end

    statement.close

    puts "Finished fetching clusters (#{Time.now - start})"

    clusters.keys.sort
  end

  def query_users(cluster)
    db_conn = UsageDatabase.connection

    puts "Fetching users for #{cluster}"
    start = Time.now

    users = {}

    statement = db_conn.create_statement
    result = statement.execute_query("SELECT DISTINCT userName FROM Usage WHERE cluster='#{cluster}';")

    while result.next do
      user_name = result.get_string(1)
      users[user_name] = true
    end

    statement.close

    puts "Finished fetching users (#{Time.now - start})"

    users.keys.sort
  end

  def query_by_time(what,where,measures,time_unit,time_zone)
    puts "Querying:"
    puts "* what: #{what}"
    puts "* where: #{where}"
    puts "* measures: #{measures}"
    puts "* time unit: #{time_unit}"
    puts "* time zone: #{time_zone}"

    # Java::java.util.TimeZone.getAvailableIDs.each do |tzone|
    #   puts tzone
    # end

    db_conn = UsageDatabase.connection

    cube = Cube.new(what)

    measures_string = measures.map { |m| "SUM(#{m})"}.join(",")

    what_string = what.map do |w| 
      if w == :time
        case time_unit
        when "HOURS"
          "time" # already rounded to hours by Hadoop jobs
        when "DAYS"
          "roundTimestampToDay(time,'#{time_zone}')"
        when "WEEKS"
          "roundTimestampToWeek(time,'#{time_zone}')"
        when "MONTHS"
          "roundTimestampToMonth(time,'#{time_zone}')"
        else
          raise "Unrecognized unit: #{unit}"
        end        
      else
        w.to_s 
      end
    end.join(",")

    where_string = where.map do |dimension,condition|
      operator = condition.first[0]
      operand = condition.first[1]

      case operator
      when :equals
        Proc.new { |val| 
          val == operand
        }
        "#{dimension}='#{operand}'"
      # when :in
      #   operand = Set.new(operand)
      #   Proc.new { |val| 
      #     operand.include?(val) 
      #   }
      else
        raise "Unknown operator: #{operator}"
      end
    end.join(" AND ")

    query_string = %|
      SELECT #{what_string},#{measures_string}
      FROM Usage
      WHERE #{where_string}
      GROUP BY #{what_string};|

    puts query_string

    statement = db_conn.create_statement
    puts "Executing query"
    result = statement.execute_query(query_string)
    puts "Done!"

    tz = TZInfo::Timezone.get(time_zone)

    puts "Aggregating data returned by query"

    while result.next do
      keys = {}
      values = {}

      i = 1
      what.each do |w|

        v = result.get_object(i)

        keys[w] = case w

        # Map the time to the local aggregation time value.  
        when :time
          rounded_time = result.get_object(i)
          rounded_time.time
        else 
          v
        end

        i += 1
      end

      measures.each do |m|
        v = result.get_object(i)

        values[m] = case v
        when Java::JavaMath::BigDecimal
          v.longValue
        else
          v
        end

        i += 1
      end

      # keys should be unique because of SQL GROUP BY clause, so can put! instead of aggregate!
      cube.put!(keys.values,values)
    end

    puts "Done!"

    statement.close

    cube
  end

end
