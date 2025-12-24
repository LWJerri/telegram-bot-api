//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2025
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#pragma once

#include "td/utils/common.h"
#include "td/utils/Slice.h"
#include "td/utils/Status.h"

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace telegram_bot_api {

struct S3Config {
  td::string bucket;
  td::string region;
  td::string access_key_id;
  td::string secret_access_key;
  td::string endpoint;
  td::string path_prefix;
  bool use_path_style = false;
  bool use_public_urls = false;
  bool use_path_only = false;
  td::int32 presigned_url_expiry_seconds = 3600;

  bool is_enabled() const {
    return !bucket.empty() && !access_key_id.empty() && !secret_access_key.empty();
  }
};

class S3StreamingUpload;

class S3Storage {
 public:
  explicit S3Storage(const S3Config &config);
  ~S3Storage();

  S3Storage(const S3Storage &) = delete;
  S3Storage &operator=(const S3Storage &) = delete;
  S3Storage(S3Storage &&) = delete;
  S3Storage &operator=(S3Storage &&) = delete;

  bool is_enabled() const;

  td::Result<td::string> upload_file(td::Slice local_path, td::Slice s3_key);

  std::unique_ptr<S3StreamingUpload> create_streaming_upload(td::Slice s3_key, td::int64 expected_size = -1);

  td::Result<td::string> get_presigned_url(td::Slice s3_key);

  td::string get_public_url(td::Slice s3_key) const;

  td::Result<td::string> get_file_url(td::Slice s3_key);

  td::string get_file_path(td::Slice s3_key) const;

  td::Status delete_file(td::Slice s3_key);

  bool file_exists(td::Slice s3_key);

  const S3Config &get_config() const {
    return config_;
  }

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  S3Config config_;

  friend class S3StreamingUpload;
};

class S3StreamingUpload {
 public:
  static constexpr td::int64 MIN_PART_SIZE = 5 * 1024 * 1024;

  enum class Status {
    NotStarted,
    InProgress,
    Completed,
    Failed,
    Aborted
  };

  ~S3StreamingUpload();

  S3StreamingUpload(const S3StreamingUpload &) = delete;
  S3StreamingUpload &operator=(const S3StreamingUpload &) = delete;
  S3StreamingUpload(S3StreamingUpload &&) noexcept;
  S3StreamingUpload &operator=(S3StreamingUpload &&) noexcept;

  td::Status init();

  td::Status upload_part(td::int64 offset, td::Slice data);

  td::Result<td::string> complete();

  td::Status abort();

  Status get_status() const {
    return status_;
  }

  const td::string &get_s3_key() const {
    return s3_key_;
  }

  td::int64 get_uploaded_bytes() const {
    return uploaded_bytes_;
  }

  bool is_active() const {
    return status_ == Status::NotStarted || status_ == Status::InProgress;
  }

 private:
  friend class S3Storage;
  
  S3StreamingUpload(S3Storage::Impl *storage_impl, const S3Config &config, td::string s3_key, td::int64 expected_size);

  class Impl;
  std::unique_ptr<Impl> impl_;
  S3Storage::Impl *storage_impl_;
  S3Config config_;
  td::string s3_key_;
  td::int64 expected_size_;
  td::int64 uploaded_bytes_ = 0;
  Status status_ = Status::NotStarted;
};

using StreamingUploadFactory = std::function<std::unique_ptr<S3StreamingUpload>(td::Slice s3_key, td::int64 expected_size)>;

using StreamingDataCallback = std::function<td::Status(td::int64 offset, td::Slice data)>;

}  // namespace telegram_bot_api
