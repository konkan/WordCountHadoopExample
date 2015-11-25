using System;
using System.IO;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace HadoopLab
{
    /// <summary>
    /// Used to initialize Azure blob storage with data.
    /// </summary>
    public class AzureStorageInitiator
    {
        private readonly CloudBlobClient _blobClient;
        private readonly Regex _storageUriRegex = new Regex(@"\/(?<containerName>[^\/]+)\/(?<fileName>.+)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

        public AzureStorageInitiator(string connectionString)
        {
            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);

            _blobClient = cloudStorageAccount.CreateCloudBlobClient();
        }

        public async Task InitStorageAsync()
        {
            //Create "virtual" directories
            CreateBlockBlob("user/test");
            CreateBlockBlob("user/test/counter");
            CreateBlockBlob("user/test/counter/input");

            //Upload test data file
            var assemblyLocation = Assembly.GetExecutingAssembly().Location;
            var testDataPath = Path.GetDirectoryName(assemblyLocation) + "\\TestData.txt";
            await UploadAsync("user/user/test/counter/input", testDataPath);
        }

        private void CreateBlockBlob(string path)
        {
            var container = _blobClient.GetContainerReference("user");

            var userBlock = container.GetBlockBlobReference(path);
            userBlock.Metadata.Add("hdi_isfolder", "hdi_isfolder"); //Maybe not needed
            userBlock.UploadText(string.Empty);
        }

        private async Task UploadAsync(string blobDirectoryPath, string filePath)
        {
            var blobDirectoryUri = new Uri(_blobClient.StorageUri.PrimaryUri.OriginalString + blobDirectoryPath);
            var container = GetContainer(blobDirectoryUri);

            if (container == null)
            {
                Console.WriteLine("Cannot find container for path " + blobDirectoryUri);
                return;
            }

            var blockBlob = GetBlockBlobReference(blobDirectoryUri, filePath, container);

            using (var localFileStream = File.OpenRead(filePath))
            {
                await blockBlob.UploadFromStreamAsync(localFileStream);
            }
        }

        private CloudBlobContainer GetContainer(Uri fileUri)
        {
            var match = _storageUriRegex.Match(fileUri.AbsolutePath);

            if (match.Success == false)
                throw new Exception("Faulty URI");

            var containerName = match.Groups["containerName"].Value;

            return _blobClient.GetContainerReference(containerName);
        }

        private static CloudBlockBlob GetBlockBlobReference(Uri blobDirectoryUri, string filePath, CloudBlobContainer container)
        {
            var length = ("/" + container.Name + "/").Length;
            var startPath = blobDirectoryUri.AbsolutePath.Substring(length);

            if (startPath.EndsWith("/") == false)
                startPath += "/";

            var blockName = startPath + Path.GetFileName(filePath);

            return container.GetBlockBlobReference(blockName);
        }
    }
}