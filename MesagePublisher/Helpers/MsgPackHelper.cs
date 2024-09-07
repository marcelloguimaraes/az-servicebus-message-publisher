using MsgPack.Serialization;

namespace MesagePublisher.Helpers;

public static class MsgPackHelper
{
    private static readonly SerializationContext _context = new() { SerializationMethod = SerializationMethod.Map };

    public static byte[]? SerializeCompressed(object value)
    {
        return GzipHelper.Compress(Serialize(value));
    }

    private static byte[]? Serialize(object value)
    {
        if (value == null)
        {
            return null;
        }

        using var byteStream = new MemoryStream();
        var objectType = value.GetType();
        var serializer = MessagePackSerializer.Get(objectType, _context);
        serializer.Pack(byteStream, value);
        return byteStream.ToArray();

    }
}