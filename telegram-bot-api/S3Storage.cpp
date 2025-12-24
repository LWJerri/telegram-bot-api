//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2025
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "telegram-bot-api/S3Storage.h"

#include "td/utils/filesystem.h"
#include "td/utils/logging.h"
#include "td/utils/misc.h"
#include "td/utils/PathView.h"
#include "td/utils/port/FileFd.h"
#include "td/utils/Slice.h"
#include "td/utils/SliceBuilder.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/S3EndpointProvider.h>

#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>
#include <vector>

namespace telegram_bot_api {

namespace {
std::once_flag aws_init_flag;
bool aws_initialized = false;

void init_aws_sdk() {
  std::call_once(aws_init_flag, []() {
    Aws::SDKOptions options;
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Warn;
    Aws::InitAPI(options);
    aws_initialized = true;
    LOG(INFO) << "AWS SDK initialized";
  });
}

td::string detect_content_type(td::Slice path) {
  auto ext = td::PathView(path).extension();
  if (ext == "jpg" || ext == "jpeg") {
    return "image/jpeg";
  } else if (ext == "png") {
    return "image/png";
  } else if (ext == "gif") {
    return "image/gif";
  } else if (ext == "webp") {
    return "image/webp";
  } else if (ext == "mp4") {
    return "video/mp4";
  } else if (ext == "webm") {
    return "video/webm";
  } else if (ext == "mp3") {
    return "audio/mpeg";
  } else if (ext == "ogg") {
    return "audio/ogg";
  } else if (ext == "pdf") {
    return "application/pdf";
  } else if (ext == "json") {
    return "application/json";
  }
  return "application/octet-stream";
}
}

class S3Storage::Impl {
 public:
  explicit Impl(const S3Config &config) : config_(config) {
    init_aws_sdk();

    Aws::Client::ClientConfiguration client_config;
    client_config.region = config_.region.c_str();

    if (!config_.endpoint.empty()) {
      client_config.endpointOverride = config_.endpoint.c_str();
    }

    Aws::Auth::AWSCredentials credentials(config_.access_key_id.c_str(), config_.secret_access_key.c_str());

    Aws::S3::S3ClientConfiguration s3_config(client_config);
    s3_config.useVirtualAddressing = !config_.use_path_style;

    client_ = std::make_unique<Aws::S3::S3Client>(credentials, nullptr, s3_config);
    LOG(INFO) << "S3 client initialized for bucket: " << config_.bucket;
  }

  ~Impl() = default;

  Aws::S3::S3Client *get_client() {
    return client_.get();
  }

  td::Result<td::string> upload_file(td::Slice local_path, td::Slice s3_key) {
    std::string full_key = get_full_key(s3_key);

    auto file_content = td::read_file(local_path.str());
    if (file_content.is_error()) {
      return td::Status::Error(PSLICE() << "Failed to read file: " << file_content.error().message());
    }

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(config_.bucket.c_str());
    request.SetKey(full_key.c_str());

    auto input_data = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream");
    auto content = file_content.move_as_ok();
    input_data->write(content.data(), static_cast<std::streamsize>(content.size()));
    request.SetBody(input_data);
    request.SetContentLength(static_cast<long long>(content.size()));

    auto content_type = detect_content_type(s3_key);
    if (!content_type.empty()) {
      request.SetContentType(content_type.c_str());
    }

    auto outcome = client_->PutObject(request);
    if (!outcome.IsSuccess()) {
      return td::Status::Error(PSLICE() << "S3 upload failed: " << outcome.GetError().GetMessage().c_str());
    }

    LOG(INFO) << "Successfully uploaded file to S3: " << full_key;
    return full_key;
  }

  td::Result<td::string> get_presigned_url(td::Slice s3_key) {
    std::string full_key = get_full_key(s3_key);

    auto url = client_->GeneratePresignedUrl(config_.bucket.c_str(), full_key.c_str(), Aws::Http::HttpMethod::HTTP_GET,
                                              config_.presigned_url_expiry_seconds);

    if (url.empty()) {
      return td::Status::Error("Failed to generate presigned URL");
    }

    return td::string(url.c_str());
  }

  td::string get_public_url(td::Slice s3_key) const {
    std::string full_key = get_full_key(s3_key);

    if (!config_.endpoint.empty()) {
      if (config_.use_path_style) {
        return PSTRING() << config_.endpoint << "/" << config_.bucket << "/" << full_key;
      } else {
        auto endpoint = config_.endpoint;

        if (td::begins_with(endpoint, "https://")) {
          endpoint = endpoint.substr(8);
        } else if (td::begins_with(endpoint, "http://")) {
          endpoint = endpoint.substr(7);
        }
        return PSTRING() << "https://" << config_.bucket << "." << endpoint << "/" << full_key;
      }
    }

    // AWS S3 URL
    return PSTRING() << "https://" << config_.bucket << ".s3." << config_.region << ".amazonaws.com/" << full_key;
  }

  td::Status delete_file(td::Slice s3_key) {
    std::string full_key = get_full_key(s3_key);

    Aws::S3::Model::DeleteObjectRequest request;
    request.SetBucket(config_.bucket.c_str());
    request.SetKey(full_key.c_str());

    auto outcome = client_->DeleteObject(request);
    if (!outcome.IsSuccess()) {
      return td::Status::Error(PSLICE() << "S3 delete failed: " << outcome.GetError().GetMessage().c_str());
    }

    LOG(INFO) << "Successfully deleted file from S3: " << full_key;
    return td::Status::OK();
  }

  bool file_exists(td::Slice s3_key) {
    std::string full_key = get_full_key(s3_key);

    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(config_.bucket.c_str());
    request.SetKey(full_key.c_str());

    auto outcome = client_->HeadObject(request);
    return outcome.IsSuccess();
  }

  std::string get_full_key(td::Slice s3_key) const {
    if (config_.path_prefix.empty()) {
      return s3_key.str();
    }
    return PSTRING() << config_.path_prefix << "/" << s3_key;
  }

 private:
  S3Config config_;
  std::unique_ptr<Aws::S3::S3Client> client_;
};

class S3StreamingUpload::Impl {
 public:
  Impl(S3Storage::Impl *storage_impl, const S3Config &config, td::string full_key)
      : storage_impl_(storage_impl), config_(config), full_key_(std::move(full_key)) {
  }

  td::Status init() {
    Aws::S3::Model::CreateMultipartUploadRequest request;
    request.SetBucket(config_.bucket.c_str());
    request.SetKey(full_key_.c_str());

    auto content_type = detect_content_type(full_key_);
    if (!content_type.empty()) {
      request.SetContentType(content_type.c_str());
    }

    auto outcome = storage_impl_->get_client()->CreateMultipartUpload(request);
    if (!outcome.IsSuccess()) {
      return td::Status::Error(PSLICE() << "Failed to create multipart upload: "
                                         << outcome.GetError().GetMessage().c_str());
    }

    upload_id_ = outcome.GetResult().GetUploadId().c_str();
    LOG(INFO) << "Started multipart upload for " << full_key_ << " with upload ID: " << upload_id_;
    return td::Status::OK();
  }

  td::Status upload_part(td::int64 offset, td::Slice data) {
    buffered_data_.append(data.data(), data.size());

    while (buffered_data_.size() >= S3StreamingUpload::MIN_PART_SIZE) {
      TRY_STATUS(flush_buffer_part(false));
    }
    
    return td::Status::OK();
  }

  td::Status flush_buffer_part(bool is_final) {
    if (buffered_data_.empty()) {
      return td::Status::OK();
    }

    size_t part_size;
    if (is_final) {
      part_size = buffered_data_.size();
    } else if (buffered_data_.size() >= S3StreamingUpload::MIN_PART_SIZE) {
      part_size = S3StreamingUpload::MIN_PART_SIZE;
    } else {
      return td::Status::OK();
    }

    if (part_size == 0) {
      return td::Status::OK();
    }

    int part_number = static_cast<int>(completed_parts_.size()) + 1;

    Aws::S3::Model::UploadPartRequest request;
    request.SetBucket(config_.bucket.c_str());
    request.SetKey(full_key_.c_str());
    request.SetUploadId(upload_id_.c_str());
    request.SetPartNumber(part_number);
    request.SetContentLength(static_cast<long long>(part_size));

    auto input_data = Aws::MakeShared<Aws::StringStream>("UploadPartInputStream");
    input_data->write(buffered_data_.data(), static_cast<std::streamsize>(part_size));
    request.SetBody(input_data);

    auto outcome = storage_impl_->get_client()->UploadPart(request);
    if (!outcome.IsSuccess()) {
      return td::Status::Error(PSLICE() << "Failed to upload part " << part_number << ": "
                                         << outcome.GetError().GetMessage().c_str());
    }

    Aws::S3::Model::CompletedPart completed_part;
    completed_part.SetPartNumber(part_number);
    completed_part.SetETag(outcome.GetResult().GetETag());
    completed_parts_.push_back(std::move(completed_part));

    buffered_data_.erase(0, part_size);

    LOG(DEBUG) << "Uploaded part " << part_number << " (" << part_size << " bytes) for " << full_key_;
    return td::Status::OK();
  }

  td::Result<td::string> complete() {
    TRY_STATUS(flush_buffer_part(true));

    if (completed_parts_.empty()) {
      abort().ignore();

      return td::Status::Error("No data was uploaded");
    }

    Aws::S3::Model::CompleteMultipartUploadRequest request;
    request.SetBucket(config_.bucket.c_str());
    request.SetKey(full_key_.c_str());
    request.SetUploadId(upload_id_.c_str());

    Aws::S3::Model::CompletedMultipartUpload completed_upload;
    completed_upload.SetParts(Aws::Vector<Aws::S3::Model::CompletedPart>(completed_parts_.begin(), completed_parts_.end()));
    request.SetMultipartUpload(completed_upload);

    auto outcome = storage_impl_->get_client()->CompleteMultipartUpload(request);
    if (!outcome.IsSuccess()) {
      return td::Status::Error(PSLICE() << "Failed to complete multipart upload: "
                                         << outcome.GetError().GetMessage().c_str());
    }

    LOG(INFO) << "Completed multipart upload for " << full_key_ << " with " << completed_parts_.size() << " parts";
    return full_key_;
  }

  td::Status abort() {
    if (upload_id_.empty()) {
      return td::Status::OK();
    }

    Aws::S3::Model::AbortMultipartUploadRequest request;
    request.SetBucket(config_.bucket.c_str());
    request.SetKey(full_key_.c_str());
    request.SetUploadId(upload_id_.c_str());

    auto outcome = storage_impl_->get_client()->AbortMultipartUpload(request);
    if (!outcome.IsSuccess()) {
      LOG(WARNING) << "Failed to abort multipart upload: " << outcome.GetError().GetMessage().c_str();
    } else {
      LOG(INFO) << "Aborted multipart upload for " << full_key_;
    }

    upload_id_.clear();
    completed_parts_.clear();
    buffered_data_.clear();
    return td::Status::OK();
  }

 private:
  S3Storage::Impl *storage_impl_;
  S3Config config_;
  td::string full_key_;
  td::string upload_id_;
  std::vector<Aws::S3::Model::CompletedPart> completed_parts_;
  std::string buffered_data_;
};

S3StreamingUpload::S3StreamingUpload(S3Storage::Impl *storage_impl, const S3Config &config, 
                                       td::string s3_key, td::int64 expected_size)
    : storage_impl_(storage_impl)
    , config_(config)
    , s3_key_(std::move(s3_key))
    , expected_size_(expected_size) {
  auto full_key = storage_impl_->get_full_key(s3_key_);
  impl_ = std::make_unique<Impl>(storage_impl_, config_, std::move(full_key));
}

S3StreamingUpload::~S3StreamingUpload() {
  if (status_ == Status::InProgress) {
    abort().ignore();
  }
}

S3StreamingUpload::S3StreamingUpload(S3StreamingUpload &&other) noexcept = default;
S3StreamingUpload &S3StreamingUpload::operator=(S3StreamingUpload &&other) noexcept = default;

td::Status S3StreamingUpload::init() {
  if (status_ != Status::NotStarted) {
    return td::Status::Error("Upload already started");
  }
  auto status = impl_->init();
  if (status.is_error()) {
    status_ = Status::Failed;
    return status;
  }
  status_ = Status::InProgress;
  return td::Status::OK();
}

td::Status S3StreamingUpload::upload_part(td::int64 offset, td::Slice data) {
  if (status_ != Status::InProgress) {
    return td::Status::Error("Upload not in progress");
  }
  auto status = impl_->upload_part(offset, data);
  if (status.is_error()) {
    status_ = Status::Failed;
    return status;
  }
  uploaded_bytes_ += data.size();
  return td::Status::OK();
}

td::Result<td::string> S3StreamingUpload::complete() {
  if (status_ != Status::InProgress) {
    return td::Status::Error("Upload not in progress");
  }
  auto result = impl_->complete();
  if (result.is_error()) {
    status_ = Status::Failed;
    return result.move_as_error();
  }
  status_ = Status::Completed;
  return result.move_as_ok();
}

td::Status S3StreamingUpload::abort() {
  if (status_ == Status::Completed || status_ == Status::Aborted) {
    return td::Status::OK();
  }
  auto status = impl_->abort();
  status_ = Status::Aborted;
  return status;
}

S3Storage::S3Storage(const S3Config &config) : config_(config) {
  if (config_.is_enabled()) {
    impl_ = std::make_unique<Impl>(config);
  }
}

S3Storage::~S3Storage() = default;

bool S3Storage::is_enabled() const {
  return impl_ != nullptr;
}

td::Result<td::string> S3Storage::upload_file(td::Slice local_path, td::Slice s3_key) {
  if (!impl_) {
    return td::Status::Error("S3 storage is not enabled");
  }
  return impl_->upload_file(local_path, s3_key);
}

std::unique_ptr<S3StreamingUpload> S3Storage::create_streaming_upload(td::Slice s3_key, td::int64 expected_size) {
  if (!impl_) {
    return nullptr;
  }
  return std::unique_ptr<S3StreamingUpload>(
      new S3StreamingUpload(impl_.get(), config_, s3_key.str(), expected_size));
}

td::Result<td::string> S3Storage::get_presigned_url(td::Slice s3_key) {
  if (!impl_) {
    return td::Status::Error("S3 storage is not enabled");
  }
  return impl_->get_presigned_url(s3_key);
}

td::string S3Storage::get_public_url(td::Slice s3_key) const {
  if (!impl_) {
    return td::string();
  }
  return impl_->get_public_url(s3_key);
}

td::Result<td::string> S3Storage::get_file_url(td::Slice s3_key) {
  if (!impl_) {
    return td::Status::Error("S3 storage is not enabled");
  }
  if (config_.use_path_only) {
    return impl_->get_full_key(s3_key);
  }
  if (config_.use_public_urls) {
    return impl_->get_public_url(s3_key);
  }
  return impl_->get_presigned_url(s3_key);
}

td::string S3Storage::get_file_path(td::Slice s3_key) const {
  if (!impl_) {
    return td::string();
  }
  return impl_->get_full_key(s3_key);
}

td::Status S3Storage::delete_file(td::Slice s3_key) {
  if (!impl_) {
    return td::Status::Error("S3 storage is not enabled");
  }
  return impl_->delete_file(s3_key);
}

bool S3Storage::file_exists(td::Slice s3_key) {
  if (!impl_) {
    return false;
  }
  return impl_->file_exists(s3_key);
}

}  // namespace telegram_bot_api
