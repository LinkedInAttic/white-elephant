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

require 'app/usage_database'
require 'app/usage_data'
require 'app/usage_file_load_task'

class UsageLoader < Java::java.lang.Thread

  def initialize(config)
    super()
    @config = config
  end

  def run
    begin
      puts "Running usage loader"
      refresh_in_ms = begin
        mins = @config["refresh_in_mins"]
        (mins*60*1000).to_i
      end
      puts "Refresh every #{refresh_in_ms} ms"
    rescue StandardError => error 
      puts "Failed to start usage loader thread: #{error}"
      raise error
    end

    loop do
      begin
        puts "Loading avro data"
        before_load
        load_avro_files
        after_load

      # prevent known exception from killing our thread
      rescue StandardError, Java::java.io.IOException => error
        puts "Failed to load avro data: #{error}"
      
      ensure 
        puts "Waiting #{refresh_in_ms} ms before next refresh"
        Java::java.lang.Thread.sleep(refresh_in_ms)
      end
    end
  end

  def before_load
  end

  def after_load
  end

  def get_file_status(file,modified_time)
    prepared_statement = UsageFileLoadTask.conn.get.prepareStatement("SELECT modified FROM LoadedFiles WHERE fileName=?")
    prepared_statement.set_string(1,file)
    result = prepared_statement.execute_query
    status = :unprocessed
    if result.next      
      timestamp = result.get_timestamp(1)
      puts "Comparing #{timestamp.get_time.to_i} to #{modified_time.to_i}"
      if timestamp.get_time.to_i != modified_time.to_i*1000
        puts "Already loaded #{file}, however previous modified time #{timestamp} does not match new modified time #{modified_time}"
        status = :modified
      else
        puts "Already loaded #{file} with modified time #{timestamp}"
        status = :processed
      end
    end
    prepared_statement.close
    status
  end

  def prepare_files_to_process(files)
    files_to_process = []

    files.each do |file_name,modified_time|
      prepared_statement = UsageFileLoadTask.conn.get.prepareStatement("SELECT modified FROM LoadedFiles WHERE fileName=?")
      prepared_statement.set_string(1,file_name)
      result = prepared_statement.execute_query
      delete_file = false
      if result.next      
        timestamp = result.get_timestamp(1)
        if timestamp.get_time.to_i != modified_time.to_i*1000
          puts "Already loaded #{file_name}, however modified times do not match"
          delete_file = true
          files_to_process << [file_name,modified_time]
        end
      else
        files_to_process << [file_name,modified_time]
      end
      prepared_statement.close
      
      if delete_file
        # Deleting the file record also deletes the associated usage data.  This data will be reloaded when the file record
        # is inserted again.
        puts "Deleting record of #{file_name} and associated usage data"
        prepared_statement = UsageFileLoadTask.conn.get.prepareStatement("DELETE FROM LoadedFiles WHERE fileName=?")
        prepared_statement.set_string(1,file_name)
        prepared_statement.execute_update
        prepared_statement.close
      end
    end

    files_to_process
  end

  def get_file_id(file)
    prepared_statement = UsageFileLoadTask.conn.get.prepareStatement("SELECT id FROM LoadedFiles WHERE fileName=?")
    prepared_statement.set_string(1,file)
    result = prepared_statement.execute_query
    if result.next
      result.get_int(1)
    end
  end

  def record_processed_file(file,modified_time)
    prepared_statement = UsageFileLoadTask.conn.get.prepareStatement("INSERT INTO LoadedFiles (fileName,modified) VALUES (?,?)")
    prepared_statement.set_string(1,file)
    prepared_statement.set_timestamp(2,Java::java.sql.Timestamp.new(modified_time.to_i*1000))
    prepared_statement.execute_update
    prepared_statement.close
  end

  # delete data from DB where corresponding file no longer exists
  def check_for_deleted_files(files)
    files = files.map { |file| file[0] }

    file_set = {}
    files.each { |file_name| file_set[file_name] = true }

    prepared_statement = UsageFileLoadTask.conn.get.prepareStatement("SELECT fileName FROM LoadedFiles")
    result = prepared_statement.execute_query
    files_to_delete = []
    while result.next
      file_name = result.get_string(1)
      unless file_set.key? file_name
        files_to_delete << file_name
      end
    end
    prepared_statement.close

    files_to_delete.each do |file|
      puts "Removing data for #{file}"
      prepared_statement = UsageFileLoadTask.conn.get.prepareStatement("DELETE FROM LoadedFiles WHERE fileName=?")
      prepared_statement.set_string(1,file)
      prepared_statement.execute_update
    end

    if files_to_delete.size > 0
      puts "Invalidating the usage data cache"
      UsageData.clear_cache
    end
  end

  def load_avro_files    
    start = Time.now

    files = list_files

    check_for_deleted_files(files)

    files = prepare_files_to_process(files)

    if files.size > 0
      puts "Found #{files.size} files to process"

      executor = Java::java.util.concurrent.Executors.newFixedThreadPool(4)

      futures = []

      files.each do |file_name,modified_time|
        file_id = get_file_id(file_name)
        local_file_name = get_local_file(file_name)
        futures << executor.submit(UsageFileLoadTask.new(file_name,modified_time,local_file_name))
      end

      executor.shutdown

      until executor.awaitTermination(2,Java::java.util.concurrent.TimeUnit::SECONDS) do
        num_finished = futures.count { |f| f.isDone }
        puts "Loading data (#{num_finished.to_f/futures.size*100}% complete)"
      end

      puts "Done loading data!  That took #{(Time.now - start).to_i} seconds"
      puts "Invalidating the usage data cache"
      UsageData.clear_cache
    else
      puts "No files to process"
    end

  rescue Exception => ex
    puts "Failed: #{ex}"
    puts ex.backtrace.join("\n")
    raise ex
  end
end

