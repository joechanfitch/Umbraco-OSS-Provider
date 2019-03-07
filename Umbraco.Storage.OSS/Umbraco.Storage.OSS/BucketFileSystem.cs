using Aliyun.OSS;
using Aliyun.OSS.Common;
using Aliyun.OSS.Util;
using System;
using System.Configuration;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Umbraco.Core.IO;

namespace Umbraco.Storage.OSS
{
    public class BucketFileSystem : IFileSystem
    {
        protected readonly string BucketName;
        protected readonly string BucketHostName;
        protected readonly string BucketHostURL;
        protected readonly string BucketPrefix;
        protected const string Delimiter = "/";

        protected const int BatchSize = 1000;
        public Func<IOss> ClientFactory { get; set; }

        public BucketFileSystem(
            string bucketName,
            string bucketHostName,
            string bucketKeyPrefix
        )
        {
            BucketName = bucketName;
            BucketHostName = bucketHostName;
            BucketPrefix = String.Concat(bucketKeyPrefix, "/");


            BucketHostURL = String.Concat("https://", BucketHostName, "/");

            string endpoint = ConfigurationManager.AppSettings["AWSEndpoint"];
            string accessKeyId = ConfigurationManager.AppSettings["AWSAccessKey"];
            string accessKeySecret = ConfigurationManager.AppSettings["AWSSecretKey"];

            ClientFactory = () => new OssClient(endpoint, accessKeyId, accessKeySecret);
        }

        protected virtual T Execute<T>(Func<IOss, T> request)
        {
            var client = ClientFactory();
            try
            {
                return request(client);
            }
            catch (OssException ex)
            {
                Console.WriteLine("Failed with error code: {0}; Error info: {1}. \nRequestID:{2}\tHostID:{3}",
                    ex.ErrorCode, ex.Message, ex.RequestId, ex.HostId);
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed with error info: {0}", ex.Message);
                throw;
            }
        }

        protected virtual IEnumerable<ObjectListing> ExecuteWithContinuation(ListObjectsRequest request)
        {
            var response = Execute(client => client.ListObjects(request));
            yield return response;

            while (response.IsTruncated)
            {
                request.Marker = response.NextMarker;
                response = Execute(client => client.ListObjects(request));
                yield return response;
            }
        }

        protected virtual string ResolveBucketPath(string path, bool isDir = false)
        {
            if (string.IsNullOrEmpty(path))
                return BucketPrefix;

            //Remove Bucket Hostname
            if (!path.Equals("/") && path.StartsWith(BucketHostName, StringComparison.InvariantCultureIgnoreCase))
                path = path.Substring(BucketHostName.Length);

            path = path.Replace("\\", Delimiter);
            if (path == Delimiter)
                return BucketPrefix;

            if (path.StartsWith(Delimiter))
                path = path.Substring(1);

            //Remove Key Prefix If Duplicate
            if (path.StartsWith(BucketPrefix, StringComparison.InvariantCultureIgnoreCase))
                path = path.Substring(BucketPrefix.Length);

            if (isDir && (!path.EndsWith(Delimiter)))
                path = string.Concat(path, Delimiter);

            return string.Concat(BucketPrefix, path);
        }

        protected virtual string RemovePrefix(string key)
        {
            if (!string.IsNullOrEmpty(BucketPrefix) && key.StartsWith(BucketPrefix))
                key = key.Substring(BucketPrefix.Length);

            if (key.EndsWith(Delimiter))
                key = key.Substring(0, key.Length - Delimiter.Length);
            return key;
        }

        public virtual void AddFile(String path, Stream stream)
        {
            AddFile(path, stream, true);
        }
        public virtual void AddFile(String path, Stream stream, bool overrideIfExists)
        {

            using (var memoryStream = new MemoryStream())
            {
                stream.CopyTo(memoryStream);
                memoryStream.Seek(0, SeekOrigin.Begin);

                var key = ResolveBucketPath(path);

                var request = new PutObjectRequest(
                    BucketName,
                    key,
                    memoryStream
                );

                var response = Execute(client => client.PutObject(request));
            }
        }
        public virtual void DeleteDirectory(string path)
        {
            DeleteDirectory(path, false);
        }
        public virtual void DeleteDirectory(string path, bool recursive)
        {
            //List Objects To Delete
            var listRequest = new ListObjectsRequest(BucketName);
            listRequest.Prefix = ResolveBucketPath(path, true);

            var listResponse = ExecuteWithContinuation(listRequest);
            var keys = listResponse
                .SelectMany(p => p.ObjectSummaries)
                .Select(p => p.Key)
                .ToArray();

            //Batch Deletion Requests
            var request = new DeleteObjectsRequest(BucketName, keys);
            Execute(client => client.DeleteObjects(request));
        }

        public virtual void DeleteFile(string path)
        {
            var keys = new List<string>();
            keys.Add(ResolveBucketPath(path));
            var request = new DeleteObjectsRequest(BucketName, keys);
            Execute(client => client.DeleteObjects(request));
        }

        public virtual bool DirectoryExists(string path)
        {
            var request = new ListObjectsRequest(BucketName);
            request.Prefix = ResolveBucketPath(path, true);
            request.MaxKeys = 1;

            var response = Execute(client => client.ListObjects(request));
            return response.ObjectSummaries.Count() > 0;
        }
        public virtual bool FileExists(string path)
        {
            var request = new GetObjectRequest(BucketName, ResolveBucketPath(path));

            try
            {
                Execute(client => client.GetObject(request));
                return true;
            }
            catch (FileNotFoundException)
            {
                return false;
            }
        }
        public virtual DateTimeOffset GetCreated(string path)
        {
            return GetLastModified(path);
        }

        public IEnumerable<string> GetDirectories(string path)
        {
            if (string.IsNullOrEmpty(path))
                path = "/";

            path = ResolveBucketPath(path, true);

            var request = new ListObjectsRequest(BucketName);
            request.Prefix = path;
            request.Delimiter = Delimiter;

            var response = ExecuteWithContinuation(request);
            return response
                .SelectMany(p => p.CommonPrefixes)
                .ToArray();
        }

        public virtual IEnumerable<string> GetFiles(string path)
        {
            return GetFiles(path, "*.*");
        }

        public virtual IEnumerable<string> GetFiles(string path, string filter)
        {
            path = ResolveBucketPath(path, true);

            string filename = Path.GetFileNameWithoutExtension(filter);
            if (filename.EndsWith("*"))
                filename = filename.Remove(filename.Length - 1);

            string ext = Path.GetExtension(filter);
            if (ext.Contains("*"))
                ext = string.Empty;

            var request = new ListObjectsRequest(BucketName);
            request.Prefix = path + filename;
            request.Delimiter = Delimiter;

            var response = ExecuteWithContinuation(request);
            return response
                .SelectMany(p => p.ObjectSummaries)
                .Select(p => RemovePrefix(p.Key))
                .Where(p => !string.IsNullOrEmpty(p) && p.EndsWith(ext))
                .ToArray();
        }

        public virtual string GetFullPath(string path)
        {
            return path;
        }

        public virtual DateTimeOffset GetLastModified(string path)
        {
            var request = new GetObjectRequest(BucketName, ResolveBucketPath(path));
            try
            {
                var response = Execute(client => client.GetObject(request));
                return new DateTimeOffset(response.Metadata.LastModified);
            }
            catch (FileNotFoundException)
            {
                return DateTimeOffset.MinValue;
            }
        }

        public virtual string GetRelativePath(string fullPathOrUrl)
        {
            if (string.IsNullOrEmpty(fullPathOrUrl))
                return string.Empty;

            if (fullPathOrUrl.StartsWith(Delimiter))
                fullPathOrUrl = fullPathOrUrl.Substring(1);

            //Strip Hostname
            if (fullPathOrUrl.StartsWith(BucketHostURL, StringComparison.InvariantCultureIgnoreCase))
                fullPathOrUrl = fullPathOrUrl.Substring(BucketHostURL.Length);

            //Strip Bucket Prefix
            if (fullPathOrUrl.StartsWith(BucketPrefix, StringComparison.InvariantCultureIgnoreCase))
                return fullPathOrUrl.Substring(BucketPrefix.Length);

            return fullPathOrUrl;
        }

        public virtual string GetUrl(string path)
        {
            return string.Concat(BucketHostURL, ResolveBucketPath(path));
        }

        public virtual Stream OpenFile(string path)
        {
            var request = new GetObjectRequest(BucketName, ResolveBucketPath(path));

            MemoryStream stream;
            using (var response = Execute(client => client.GetObject(request)))
            {
                stream = new MemoryStream();
                response.ResponseStream.CopyTo(stream);
            }
            stream.Seek(0, SeekOrigin.Begin);
            return stream;
        }
    }
}
