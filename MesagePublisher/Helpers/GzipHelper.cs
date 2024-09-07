namespace MesagePublisher.Helpers;

using System.IO.Compression;

public static class GzipHelper
{
    public static byte[]? Compress(byte[]? data)
    {
        if (data == null)
        {
            return null;
        }

        using (var compressedStream = new MemoryStream())
        using (var zipStream = new GZipStream(compressedStream, CompressionMode.Compress))
        {
            zipStream.Write(data, 0, data.Length);
            zipStream.Close();
            return compressedStream.ToArray();
        }
    }
}