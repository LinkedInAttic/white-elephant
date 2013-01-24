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

# It appears that Java has problems loading these unless we include them first.
java_import org.hsqldb.jdbc.JDBCDriver
java_import com.linkedin.whiteelephant.TimeZoneConversion

class UsageDatabase
  
  def initialize(key)
    @key = key
  end

  class << self

    def connection
      Java::java.sql.DriverManager.getConnection("jdbc:hsqldb:mem:mymemdb", "SA", "")
    end

    def clear_db
      puts "Clearing Usage table"
      statement = connection.create_statement
      statement.execute_update(%|
        DELETE FROM Usage
        |)
      statement.close

      puts "Clearing LoadedFiles table"
      statement = connection.create_statement
      statement.execute_update(%|
        DELETE FROM LoadedFiles
        |)
      statement.close
    end

    def initialize_db
      already_initialized = false
      statement = connection.create_statement
      result = statement.execute_query(%|SELECT TABLE_NAME FROM INFORMATION_SCHEMA.SYSTEM_TABLES where TABLE_TYPE='TABLE'|)
      while result.next
        table_name = result.get_string(1)
        if table_name == "USAGE"
          already_initialized = true
          break
        end
      end
      statement.close

      unless already_initialized
        puts "Creating tables"

        # table to hold usage summary data
        statement = connection.create_statement
        statement.execute_update(%|
          CREATE TABLE Usage 
          (
            userName varchar(12) NOT NULL,
            time TIMESTAMP NOT NULL,
            cluster varchar(12) NOT NULL,
            excess BOOLEAN NOT NULL,
            type varchar(10) NOT NULL,
            status varchar(10) NOT NULL,
            started INTEGER NOT NULL,
            finished INTEGER NOT NULL,
            elapsedMinutes DOUBLE NOT NULL,
            cpuMinutes DOUBLE NULL,
            reduceShuffleBytes BIGINT NULL,
            fileNameId INTEGER NOT NULL,
            timeMs BIGINT NOT NULL
          ) 
          |)
        statement.close

        # table to track which files have been processed
        statement = connection.create_statement
        statement.execute_update(%|
          CREATE TABLE LoadedFiles 
          (
            id INTEGER NOT NULL IDENTITY,
            fileName varchar(500) NOT NULL,
            modified TIMESTAMP NOT NULL
          ) 
          |)
        statement.close

        # file name must be unique since we'll only load them once
        statement = connection.create_statement
        statement.execute_update(%|
          ALTER TABLE LoadedFiles 
          ADD CONSTRAINT uc_LoadedFiles_fileName UNIQUE (fileName)
          |)
        statement.close

        # keep track of where data was loaded from
        statement = connection.create_statement
        statement.execute_update(%|
          ALTER TABLE Usage 
          ADD FOREIGN KEY (fileNameId) REFERENCES LoadedFiles(id)
          ON DELETE CASCADE
          |)
        statement.close

        # Ensure uniqueness of data for each file.  Data is sometimes reloaded and we must ensure it is deleted
        # from the DB before being reinserted.  Note that we use timeMs instead of time because hsqldb appears
        # to have a bug checking uniqueness with the timestamp type.  Also fileNameId is included because
        # otherwise the data will not be unique.  Each file contains data for jobs submitted on a particular day,
        # and jobs may span many hours.  Therefore files may have the same key if fileNameId is not considered.
        statement = connection.create_statement
        statement.execute_update(%|
          ALTER TABLE Usage 
          ADD CONSTRAINT uc_Usage_key UNIQUE (userName,timeMs,cluster,excess,type,status,fileNameId)
          |)
        statement.close

        puts "Creating functions"

        statement = connection.create_statement
        statement.execute_update(%|
          CREATE FUNCTION roundTimestampToDay(t TIMESTAMP, z VARCHAR(30)) RETURNS TIMESTAMP
          LANGUAGE JAVA DETERMINISTIC NO SQL
          EXTERNAL NAME 'CLASSPATH:com.linkedin.whiteelephant.TimeZoneConversion.roundTimestampToDay'

          |)
        statement.close

        statement = connection.create_statement
        statement.execute_update(%|
          CREATE FUNCTION roundTimestampToWeek(t TIMESTAMP, z VARCHAR(30)) RETURNS TIMESTAMP
          LANGUAGE JAVA DETERMINISTIC NO SQL
          EXTERNAL NAME 'CLASSPATH:com.linkedin.whiteelephant.TimeZoneConversion.roundTimestampToWeek'

          |)
        statement.close

        statement = connection.create_statement
        statement.execute_update(%|
          CREATE FUNCTION roundTimestampToMonth(t TIMESTAMP, z VARCHAR(30)) RETURNS TIMESTAMP
          LANGUAGE JAVA DETERMINISTIC NO SQL
          EXTERNAL NAME 'CLASSPATH:com.linkedin.whiteelephant.TimeZoneConversion.roundTimestampToMonth'

          |)
        statement.close
      end
    end
  end
end

