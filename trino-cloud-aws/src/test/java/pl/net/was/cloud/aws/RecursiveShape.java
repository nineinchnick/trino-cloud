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

import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.core.protocol.MarshallLocation;
import software.amazon.awssdk.core.protocol.MarshallingType;
import software.amazon.awssdk.core.traits.LocationTrait;

import java.util.List;

/**
 * None of the mapped AWS SDK models reference themselves, so this one tests the nesting depth limit.
 */
final class RecursiveShape
        implements SdkPojo
{
    static final SdkField<String> NAME = SdkField.<String>builder(MarshallingType.STRING)
            .memberName("Name")
            .traits(location("Name"))
            .getter(object -> ((RecursiveShape) object).name)
            .setter((object, value) -> ((RecursiveShape) object).name = value)
            .build();
    static final SdkField<SdkPojo> CHILD = SdkField.<SdkPojo>builder(MarshallingType.SDK_POJO)
            .memberName("Child")
            .traits(location("Child"))
            .getter(object -> ((RecursiveShape) object).child)
            .setter((object, value) -> ((RecursiveShape) object).child = (RecursiveShape) value)
            .constructor(RecursiveShape::new)
            .build();

    private String name;
    private RecursiveShape child;

    static RecursiveShape chain(int length)
    {
        RecursiveShape root = null;
        for (int level = length - 1; level >= 0; level--) {
            RecursiveShape shape = new RecursiveShape();
            shape.name = "level " + level;
            shape.child = root;
            root = shape;
        }
        return root;
    }

    RecursiveShape descendant(int level)
    {
        RecursiveShape shape = this;
        for (int i = 0; i < level; i++) {
            shape = shape.child;
        }
        return shape;
    }

    private static LocationTrait location(String name)
    {
        return LocationTrait.builder()
                .location(MarshallLocation.PAYLOAD)
                .locationName(name)
                .unmarshallLocationName(name)
                .build();
    }

    @Override
    public List<SdkField<?>> sdkFields()
    {
        return List.of(NAME, CHILD);
    }

    @Override
    public String toString()
    {
        return "RecursiveShape(Name=%s, Child=%s)".formatted(name, child);
    }
}
