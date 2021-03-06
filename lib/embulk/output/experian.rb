require "pathname"
require "fileutils"
require "tmpdir"
require "httpclient"

module Embulk
  module Output
    class Experian < OutputPlugin
      Plugin.register_output("experian", self)

      attr_reader :tmpfile

      def self.transaction(config, schema, count, &control)
        @header = schema.collect { |s| s[:name] }
        jst_time = Time.now.utc + (60 * 60 * 9)

        # configuration code:
        task = {
          tmpdir: config.param("tmpdir", :string, default: Dir.tmpdir),
          tmpfile_prefix: config.param("tmpfile_prefix", :string, default: Time.now.strftime('%Y%m%d_%H%M%S_%N_')),
          cleanup_tmpfiles: config.param("cleanup_tmpfiles", :bool, default: true),

          host: config.param("host", :string),
          site_id: config.param("site_id", :string),
          login_id: config.param("login_id", :string),
          password: config.param("password", :string),
          encoding: config.param("encoding", :string, default: "shift_jis"),
          csvfile_id: config.param("csvfile_id", :integer),
          draft_id: config.param("draft_id", :integer),
          unique_name: config.param("unique_name", :string, default: "reserved by plugin."),
          from_address: config.param("from_address", :string),

          jst_time: jst_time,
          book_year: jst_time.year,
          book_month: jst_time.month,
          book_day: jst_time.day,
          book_hour: config.param("book_hour", :integer),
          book_min: config.param("book_min", :integer)
        }

        # resumable output:
        # resume(task, schema, count, &control)

        # non-resumable output:
        FileUtils.mkdir_p task[:tmpdir]
        task_reports = yield(task)

        upload(task)
        check(task)
        # delivery_test(task)
        reserve(task)

        next_config_diff = {}
        return next_config_diff
      end

      def init
        # initialization code:
        @tmpfile = Pathname.new(task[:tmpdir]).join("#{task[:tmpfile_prefix]}_#{index}.csv")
        Embulk.logger.debug "Partial CSV will be stored as #{@tmpfile}"
      end

      def close
      end

      def add(page)
        csv = ""
        page.each do |record|
          hash = Hash[schema.names.zip(record)]
          csv_line = format_csv(hash)
          csv << csv_line << "\n"
        end
        @csv = csv
      end

      def finish
        generate_csv_file
      end

      def abort
      end

      def commit
        task_report = {}
        return task_report
      end

      private

      def format_csv(record_hash)
        record_hash.values.map do |rec|
          rec
        end.join(",")
      end

      def generate_csv_file
        File.open(@tmpfile, "a") do |f|
          f.write @csv
        end
        @csv = ""
      end

      def self.upload(task)
        csv_path = union_single_csv_file(task)
        Embulk.logger.debug "Whole CSV file path: #{csv_path}"
        Client.new(task).upload(csv_path)
        Embulk.logger.info "Uploaded csv. id:#{task[:csvfile_id]}"
      ensure
        if task[:cleanup_tmpfiles]
          tmp_path = Pathname.new(task[:tmpdir]).join(task[:tmpfile_prefix])
          Dir.glob("#{tmp_path}*.csv") do |file|
            Embulk.logger.debug "Deleting #{file} (due to `cleanup_tmpfiles` is true)"
            File.unlink file
          end
        end
      end

      def self.check(task)
        Embulk.logger.info "Checking uploaded csv. id:#{task[:csvfile_id]}"
        Client.new(task).check()
      end

      # def self.delivery_test(task)
      #   Embulk.logger.info "Delivering test mail. draft_id:#{task[:draft_id]} to:#{task[:test_address]} at:#{task[:jst_time].strftime('%Y-%m-%d %H:%M JST')}"
      #   Client.new(task).delivery_test()
      # end

      def self.reserve(task)
        Embulk.logger.info "Reserving mail. draft_id:#{task[:draft_id]} to_list:#{task[:csvfile_id]} from:#{task[:from_address]} at:#{task[:jst_time].strftime('%Y-%m-%d %H:%M JST')}"
        Client.new(task).reserve()
      end

      def self.union_single_csv_file(task)
        prefix = Pathname.new(task[:tmpdir]).join(task[:tmpfile_prefix])
        all_csv_path = "#{prefix}_all.csv"

        File.open(all_csv_path, "w", :encoding=>task[:encoding]) do |all_csv|
          all_csv.puts @header.join(",") # CSV header is required
          Dir.glob("#{prefix}*.csv") do |partial_csv|
            partial = File.read(partial_csv)

            if task[:encoding] == "shift_jis"
              partial.encode!(:encoding=>"shift_jis:utf-8", :invalid=>:replace, :undef=>:replace)
            end

            all_csv.write partial
          end
        end
        all_csv_path
      end
    end

    class Client
      attr_reader :task

      def initialize(task)
        @task = task
      end

      def upload(csv_path)
        begin
          Embulk.logger.info "csv: #{csv_path}"
          File.open(csv_path) do |csv|
            title = "#{task[:unique_name]} for draft_id:#{task[:draft_id]} at:#{task[:jst_time].strftime('%Y-%m-%d %H:%M JST')}"

            params = {
              login_id: task[:login_id],
              password: task[:password],
              id: task[:csvfile_id],
              title: title.encode!("shift_jis", :invalid=>:replace, :undef=>:replace),
              FILE: csv
            }
            if task[:encoding] == "utf-8"
              params[:list_use_utf8] = "utf-8"
            end
            url = "https://#{task[:host]}/#{task[:site_id]}/remote/upload.php"

            response = httpclient.post(url, params)
            handle_error(response)
          end
        rescue TooFrequencyError
          # NOTE: Quoted from document Sec 3.1:
          # > Request frequency
          # > The system will not receive the same type of request within 15 seconds (an error will be returned). Please allow a 15 second wait time before requests.
          Embulk.logger.warn "Got 400 error (frequency access). retry after 15 seconds"
          wait_for_retry
          retry
        end
      end

      def check()
        begin
          params = {
            login_id: task[:login_id],
            password: task[:password],
            id: task[:csvfile_id]
          }
          url = "https://#{task[:host]}/#{task[:site_id]}/remote/csvfile_list.php"
          response = httpclient.post(url, params)
          handle_error(response)
        rescue TooFrequencyError
          Embulk.logger.warn "Got 400 error (frequency access). retry after 15 seconds"
          wait_for_retry
          retry
        rescue StatusCheckError
          Embulk.logger.warn "Got Status check error. retry after 15 seconds"
          wait_for_retry
          retry
        end
      end

      def reserve()
        begin
          title = "#{task[:unique_name]} from draft_id:#{task[:draft_id]} target_id:#{task[:csvfile_id]} at:#{task[:jst_time].strftime('%Y-%m-%d %H:%M JST')}"
          params = {
            login_id: task[:login_id],
            password: task[:password],
            draft_id: task[:draft_id],
            unique_name: title.encode!("shift_jis", :invalid=>:replace, :undef=>:replace),
            from_address: task[:from_address],
            book_year: task[:book_year],
            book_month: task[:book_month],
            book_day: task[:book_day],
            book_hour: task[:book_hour],
            book_min: task[:book_min],
            csvfile_id: task[:csvfile_id]
          }
          print params
          url = "https://#{task[:host]}/#{task[:site_id]}/remote/article.php"
          response = httpclient.post(url, params)
          handle_error(response)
        rescue TooFrequencyError
          Embulk.logger.warn "Got 400 error (frequency access). retry after 15 seconds"
          wait_for_retry
          retry
        end
      end

      private

      def wait_for_retry
          sleep 15
      end

      def httpclient
        httpclient = HTTPClient.new
        # httpclient.debug_dev = STDOUT # for debugging
        httpclient
      end

      def handle_error(response)
        body = response.body.encode!(:encoding=>"shift_jis:utf-8", :invalid=>:replace, :undef=>:replace)
        case response.status
        when 200
          if body.include?("STATUS=CHECK")
            raise StatusCheckError
          end
        when 400
          if body.include?("ERROR=アクセス間隔が短すぎます。時間を置いて再度実行してください")
            raise TooFrequencyError
          else
            raise Embulk.logger.info "[#{response.status}] #{body}"
          end
        end
        Embulk.logger.info "[#{response.status}] #{body}"
      end
    end

    class TooFrequencyError < StandardError; end
    class StatusCheckError < StandardError; end
  end
end
