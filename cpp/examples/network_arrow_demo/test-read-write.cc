// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <iostream>
#include <sstream>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include "arrow/io/interfaces.h"
#include "arrow/util/macros.h"
#include "arrow/util/logging.h"
#include <curl/curl.h>

size_t CopyToMem(void *ptr, size_t size, size_t nmemb, void* stream)
{
    memcpy(stream, ptr, size*nmemb);         
}


int NetworkRead(const std::string url, uint64_t offset, uint64_t nbytes, void* out)
{
    CURL *curl = NULL;
    CURLcode res;    

    curl_global_init(CURL_GLOBAL_ALL);
    /* get a curl handle */
    curl = curl_easy_init();
    if (!curl) {
        return -1;
    }  

    std::stringstream ss;
    ss<<offset<<"-"<<offset+nbytes;

    curl_easy_setopt (curl,CURLOPT_URL, url.c_str());
    curl_easy_setopt (curl,CURLOPT_TIMEOUT, 60L);
    curl_easy_setopt (curl,CURLOPT_CONNECTTIMEOUT, 10L);
    curl_easy_setopt (curl,CURLOPT_RANGE, ss.str().c_str());

    curl_easy_setopt(curl,CURLOPT_WRITEDATA, out);
    curl_easy_setopt(curl,CURLOPT_WRITEFUNCTION, CopyToMem);


    res = curl_easy_perform(curl);
    int ret = 0;
    if (res != CURLE_OK) {
        ret = -1;
    }

    curl_easy_cleanup(curl);

    if (ret > 0) {
        return 1026;
    }
    return ret;
}

namespace arrow {
    // A RandomAccessFile that reads from a  diy object
    class ObjectInputFile : public arrow::io::RandomAccessFile {
        public:
            ObjectInputFile(const std::string path)
                : path_(path) {}

            arrow::Status Init() {

                /*
                   struct stat statbuff;
                   if( -1 == stat(path_.c_str(), &statbuff) ) {
                   return Status::Invalid("stat file fail");
                   }

                   content_length_ = statbuff.st_size;
                   */
                content_length_ = 1026;
                return Status::OK();
            }

            arrow::Status CheckClosed() const {
                if (closed_) {
                    return Status::Invalid("Operation on closed stream");
                }
                return Status::OK();
            }

            arrow::Status CheckPosition(int64_t position, const char* action) const {
                if (position < 0) {
                    return Status::Invalid("Cannot ", action, " from negative position");
                }
                if (position > content_length_) {
                    return Status::IOError("Cannot ", action, " past end of file");
                }
                return Status::OK();
            }

            // RandomAccessFile APIs

            arrow::Status Close() override {
                closed_ = true;
                content_length_ = -1;
                return Status::OK();
            }

            bool closed() const override { return closed_; }

            Result<int64_t> Tell() const override {
                RETURN_NOT_OK(CheckClosed());

                return pos_;
            }

            Result<int64_t> GetSize() override {
                RETURN_NOT_OK(CheckClosed());

                return content_length_;
            }

            arrow::Status Seek(int64_t position) override {
                RETURN_NOT_OK(CheckClosed());
                RETURN_NOT_OK(CheckPosition(position, "seek"));

                pos_ = position;
                return Status::OK();
            }

            Result<int64_t> ReadAt(int64_t position, int64_t nbytes,
                    void* out) override {
                RETURN_NOT_OK(CheckClosed());
                RETURN_NOT_OK(CheckPosition(position, "read"));



                nbytes = std::min(nbytes, content_length_ - position);
                if (nbytes == 0) {
                    return 0;
                }

                int ret = 0;

                ret = NetworkRead("http://oppomigrate-1251668577.cos.ap-guangzhou.myqcloud.com/parquet-arrow-example.parquet", position, nbytes, out);

                /*
                   int fd = -1; 
                   fd = open(path_.c_str(), O_RDONLY);
                   if (fd < 0) {
                   return fd;
                   }

                   ret = 0;
                   ret = lseek(fd, position, SEEK_SET);
                   if (ret < 0) {
                   close(fd);
                   return ret;
                   }

                   ret = read(fd, out, nbytes);
                   if (ret < 0) {
                   close(fd);
                   return ret;
                   }
                   */

                //std::cout<<"ReadAt 1 :"<<nbytes << "  "<< ret<<" bytes"<<std::endl;

                return ret;
            }

            Result<std::shared_ptr<Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
                //std::cout<<"ReadAt 2"<<std::endl;
                RETURN_NOT_OK(CheckClosed());
                RETURN_NOT_OK(CheckPosition(position, "read"));

                // No need to allocate more than the remaining number of bytes
                nbytes = std::min(nbytes, content_length_ - position);

                std::shared_ptr<ResizableBuffer> buf;
                int64_t bytes_read;
                RETURN_NOT_OK(AllocateResizableBuffer(nbytes, &buf));
                if (nbytes > 0) {
                    auto bytes_read = ReadAt(position, nbytes, buf->mutable_data());
                    //DCHECK_LE(bytes_read, nbytes);
                    //RETURN_NOT_OK(buf->Resize(bytes_read)); TODO
                }
                return std::move(buf);
                //std::shared_ptr<Buffer> a;
                //return a;
            }

            Result<int64_t> Read(int64_t nbytes, void* out) override {
                //std::cout<<"ReadAt 1-1"<<std::endl;
                return ReadAt(pos_, nbytes, out);
            }

            Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) override {
                //std::cout<<"ReadAt 2-1"<<std::endl;
                std::shared_ptr<ResizableBuffer> out; 
                //return std::move(out);
                /*
                   out = ReadAt(pos_, nbytes);
                   pos_ += (*out)->size();
                   */
                return out;
            }

        protected:
            std::string path_;
            bool closed_ = false;
            int64_t pos_ = 0;
            int64_t content_length_ = -1;
    };

}
// #0 Build dummy data to pass around
// To have some input data, we first create an Arrow Table that holds
// some data.
std::shared_ptr<arrow::Table> generate_table() {
  arrow::Int64Builder i64builder;
  PARQUET_THROW_NOT_OK(i64builder.AppendValues({1, 2, 3, 4, 5}));
  std::shared_ptr<arrow::Array> i64array;
  PARQUET_THROW_NOT_OK(i64builder.Finish(&i64array));

  arrow::StringBuilder strbuilder;
  PARQUET_THROW_NOT_OK(strbuilder.Append("some"));
  PARQUET_THROW_NOT_OK(strbuilder.Append("string"));
  PARQUET_THROW_NOT_OK(strbuilder.Append("content"));
  PARQUET_THROW_NOT_OK(strbuilder.Append("in"));
  PARQUET_THROW_NOT_OK(strbuilder.Append("rows"));
  std::shared_ptr<arrow::Array> strarray;
  PARQUET_THROW_NOT_OK(strbuilder.Finish(&strarray));

  std::shared_ptr<arrow::Schema> schema = arrow::schema(
      {arrow::field("int", arrow::int64()), arrow::field("str", arrow::utf8())});

  return arrow::Table::Make(schema, {i64array, strarray});
}

// #1 Write out the data as a Parquet file
void write_parquet_file(const arrow::Table& table) {
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile,
      arrow::io::FileOutputStream::Open("parquet-arrow-example.parquet"));
  // The last argument to the function call is the size of the RowGroup in
  // the parquet file. Normally you would choose this to be rather large but
  // for the example, we use a small value to have multiple RowGroups.
  PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, 3));
}

// #3: Read only a single RowGroup of the parquet file
void read_single_rowgroup() {
  std::cout << "Reading first RowGroup of parquet-arrow-example.parquet" << std::endl;

  auto infile = std::shared_ptr<arrow::ObjectInputFile>(new arrow::ObjectInputFile("./parquet-arrow-example.parquet"));
  PARQUET_THROW_NOT_OK(infile->Init());

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::Table> table;
  PARQUET_THROW_NOT_OK(reader->RowGroup(0)->ReadTable(&table));
  std::cout << "Loaded " << table->num_rows() << " rows in " << table->num_columns()
            << " columns." << std::endl;
}

// #4: Read only a single column of the whole parquet file
void read_single_column() {
  std::cout << "Reading first column of parquet-arrow-example.parquet" << std::endl;
  auto infile = std::shared_ptr<arrow::ObjectInputFile>(new arrow::ObjectInputFile("./parquet-arrow-example.parquet"));
  PARQUET_THROW_NOT_OK(infile->Init());

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::ChunkedArray> array;
  PARQUET_THROW_NOT_OK(reader->ReadColumn(1, &array));
  PARQUET_THROW_NOT_OK(arrow::PrettyPrint(*array, 4, &std::cout));
  std::cout << std::endl;
}

// #5: Read only a single column of a RowGroup (this is known as ColumnChunk)
//     from the Parquet file.
void read_single_column_chunk() {
  std::cout << "Reading first ColumnChunk of the first RowGroup of "
               "parquet-arrow-example.parquet"
            << std::endl;
  auto infile = std::shared_ptr<arrow::ObjectInputFile>(new arrow::ObjectInputFile("./parquet-arrow-example.parquet"));
  PARQUET_THROW_NOT_OK(infile->Init());

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::ChunkedArray> array;
  PARQUET_THROW_NOT_OK(reader->RowGroup(0)->Column(0)->Read(&array));
  PARQUET_THROW_NOT_OK(arrow::PrettyPrint(*array, 4, &std::cout));
  std::cout << std::endl;
}



void read_whole_file() {
  std::cout << "Diy Reading parquet-arrow-example.parquet at once" << std::endl;
  auto infile = std::shared_ptr<arrow::ObjectInputFile>(new arrow::ObjectInputFile("./parquet-arrow-example.parquet"));
  PARQUET_THROW_NOT_OK(infile->Init());
  
  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

  std::shared_ptr<arrow::Table> table;
  PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
  std::cout << "Loaded " << table->num_rows() << " rows in " << table->num_columns()
            << " columns." << std::endl;
}

int main(int argc, char** argv) {
  /*
  std::shared_ptr<arrow::Table> table = generate_table();
  write_parquet_file(*table);
  */
  read_whole_file();
  read_single_rowgroup();
  read_single_column();
  read_single_column_chunk();
}
