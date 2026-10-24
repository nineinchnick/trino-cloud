/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pl.net.was.cloud.aws;

import com.fasterxml.jackson.databind.module.SimpleModule;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.block.BlockJsonSerde;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;
import io.trino.type.TypeDeserializer;

import java.util.Map;
import java.util.Set;

import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;

final class HandleJsonCodecs
{
    // the engine resolves the ColumnHandle implementation from the catalog, tests can only map it statically
    private static final JsonCodecFactory CODEC_FACTORY = new JsonCodecFactory(new JsonMapperProvider()
            .withJsonSerializers(Map.of(Block.class, new BlockJsonSerde.Serializer(TESTING_BLOCK_ENCODING_SERDE)))
            .withJsonDeserializers(Map.of(
                    Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER),
                    Block.class, new BlockJsonSerde.Deserializer(TESTING_BLOCK_ENCODING_SERDE)))
            .withModules(Set.of(new SimpleModule().addAbstractTypeMapping(ColumnHandle.class, AwsColumnHandle.class)))
            .get());

    private HandleJsonCodecs() {}

    static <T> JsonCodec<T> jsonCodec(Class<T> type)
    {
        return CODEC_FACTORY.jsonCodec(type);
    }

    static <T> T roundTrip(Class<T> type, T value)
    {
        JsonCodec<T> codec = jsonCodec(type);
        return codec.fromJson(codec.toJson(value));
    }
}
