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

        # configuration code:
        task = {
          tmpdir: config.param("tmpdir", :string, default: Dir.tmpdir),
          tmpfile_prefix: config.param("tmpfile_prefix", :string, default: Time.now.strftime('%Y%m%d_%H%M%S_%N_')),
          cleanup_tmpfiles: config.param("cleanup_tmpfiles", :bool, default: true),

          site_id: config.param("site_id", :string),
          login_id: config.param("login_id", :string),
          password: config.param("password", :string),
          encoding: config.param("encoding", :string, default: "shift_jis"),
          csvfile_id: config.param("csvfile_id", :integer),
          draft_id: config.param("draft_id", :integer),
          book_time: config.param("book_time", :string),
          unique_name: config.param("unique_name", :string, default: "reserved by plugin.")
        }

        # resumable output:
        # resume(task, schema, count, &control)

        # non-resumable output:
        FileUtils.mkdir_p task[:tmpdir]
        task_reports = yield(task)

        upload(task)
        sleep 15
        check(task)
        sleep 15
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
        Client.new(task).upload_csv(csv_path)
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
        Embulk.logger.debug "checking"
        Client.new(task).check()
      end

      def self.delivery_test(task)
        Embulk.logger.debug "checking"
        Client.new(task).delivery_test()
      end

      def self.reserve(task)
        Embulk.logger.debug "checking"
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

      def upload_csv(csv_path)
        begin
          Embulk.logger.info "csv: #{csv_path}"
          File.open(csv_path) do |csv|
            params = {
              login_id: task[:login_id],
              password: task[:password],
              id: task[:csvfile_id],
              title: task[:unique_name],
              FILE: csv,
              request_id: task[:csvfile_id]
            }
            url = "https://remote2.rec.mpse.jp/#{task[:site_id]}/remote/upload.php"
            if task[:encoding] == "utf-8"
              params[:list_use_utf8] = "utf-8"
            end

            response = httpclient.post(url, params)
            handle_error(response)
          end
        rescue TooFrequencyError
          # NOTE: Quoted from document Sec 3.1:
          # > Request frequency
          # > The system will not receive the same type of request within 15 seconds (an error will be returned). Please allow a 15 second wait time before requests.
          Embulk.logger.warn "Got 400 error (frequency access). retry after 15 seconds"
          sleep 15
          retry
        end
      end

      def check()
        begin
          Embulk.logger.info "checking csvfile_id: #{task[:csvfile_id]}"

          params = {
            login_id: task[:login_id],
            password: task[:password],
            id: task[:csvfile_id],
            request_id: task[:csvfile_id]
          }
          url = "https://remote2.rec.mpse.jp/#{task[:site_id]}/remote/csvfile_list.php"
          response = httpclient.post(url, params)
          handle_error(response)
        rescue TooFrequencyError
          Embulk.logger.warn "Got 400 error (frequency access). retry after 15 seconds"
          sleep 15
          retry
        end
      end

      def delivery_test()
        begin
          Embulk.logger.info "testing delivery test: #{task[:draft_id]}"

          params = {
            login_id: task[:login_id],
            password: task[:password],
            draft_id: task[:draft_id],
            test_address: task[:test_address],
            test_subject_prefix: task[:test_subject_prefix],
          }
          url = "https://remote2.rec.mpse.jp/#{task[:site_id]}/remote/delivery_test.php"
          response = httpclient.post(url, params)
          handle_error(response)
        rescue TooFrequencyError
          Embulk.logger.warn "Got 400 error (frequency access). retry after 15 seconds"
          sleep 15
          retry
        end
      end

      def reserve()
        begin
          Embulk.logger.info "reserve draft mail: #{task[:draft_id]}"
          book_hour = task[:book_time].split(':')[0].to_i
          book_min = task[:book_time].split(':')[1].to_i
          params = {
            login_id: task[:login_id],
            password: task[:password],
            draft_id: task[:draft_id],
            unique_name: task[:unique_name],
            from_address: task[:from_address],
            book_year: Time.now.year,
            book_month: Time.now.month,
            book_day: Time.now.day,
            book_hour: book_hour,
            book_min: book_min,
            csvfile_id: task[:csvfile_id],
          }
          url = "https://remote2.rec.mpse.jp/#{task[:site_id]}/remote/article.php"
          response = httpclient.post(url, params)
          handle_error(response)
        rescue TooFrequencyError
          Embulk.logger.warn "Got 400 error (frequency access). retry after 15 seconds"
          sleep 15
          retry
        end
      end

      private

      def httpclient
        httpclient = HTTPClient.new
        # httpclient.debug_dev = STDOUT # for debugging
        httpclient
      end

      def handle_error(response)
        body = response.body.encode!(:encoding=>"shift_jis:utf-8", :invalid=>:replace, :undef=>:replace)
        Embulk.logger.debug body
        case response.status
        when 200
          # ok
          Embulk.logger.info "checking list was completed successfully."
        when 400
          if body.include?("ERROR=アクセス間隔が短すぎます。時間を置いて再度実行してください")
            raise TooFrequencyError
          elsif body.include?("STATUS=CHECK") || body.include?("STATUS=RESERVED")
            raise StatusError
          else
            raise "[#{response.status}] #{body}"
          end
        else
          raise "[#{response.status}] #{body}"
        end
      end
    end

    class TooFrequencyError < StandardError; end
    class StatusError < StandardError; end
  end
end
