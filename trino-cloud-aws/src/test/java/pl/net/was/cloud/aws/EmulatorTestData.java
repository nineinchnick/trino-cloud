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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import io.trino.testing.containers.Floci;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.Ec2ClientBuilder;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.ResourceType;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TagSpecification;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.testing.containers.Floci.FLOCI_REGION;

final class EmulatorTestData
{
    // Amazon Linux 2 image preloaded in Floci
    static final String IMAGE_ID = "ami-0abcdef1234567890";
    static final String IMAGE_OWNER_ID = "137112412989";
    static final String INSTANCE_TYPE = "t3.micro";
    static final String NAME = "trino-test";
    static final String OBJECTS_BUCKET = "trino-test-objects";
    static final String VERSIONED_BUCKET = "trino-test-versioned";
    static final String MANY_OBJECTS_BUCKET = "trino-test-many-objects";
    static final int MANY_OBJECTS_LIVE_COUNT = 1100;
    static final int MANY_OBJECTS_DELETED_COUNT = 1100;

    private final String vpcId;
    private final String subnetId;
    private final String securityGroupId;
    private final List<String> instanceIds;

    private EmulatorTestData(String vpcId, String subnetId, String securityGroupId, List<String> instanceIds)
    {
        this.vpcId = vpcId;
        this.subnetId = subnetId;
        this.securityGroupId = securityGroupId;
        this.instanceIds = ImmutableList.copyOf(instanceIds);
    }

    static EmulatorTestData create(Floci emulator)
    {
        try (S3Client s3 = emulator.createS3Client()) {
            createBuckets(s3);
        }
        Ec2ClientBuilder ec2Builder = Ec2Client.builder();
        emulator.updateClient(ec2Builder);
        try (Ec2Client ec2 = ec2Builder.build()) {
            return createEc2Resources(ec2);
        }
    }

    String vpcId()
    {
        return vpcId;
    }

    String subnetId()
    {
        return subnetId;
    }

    String securityGroupId()
    {
        return securityGroupId;
    }

    List<String> instanceIds()
    {
        return instanceIds;
    }

    private static EmulatorTestData createEc2Resources(Ec2Client ec2)
    {
        String vpcId = ec2.createVpc(request -> request
                        .cidrBlock("10.0.0.0/16")
                        .tagSpecifications(nameTag(ResourceType.VPC)))
                .vpc()
                .vpcId();
        String subnetId = ec2.createSubnet(request -> request
                        .vpcId(vpcId)
                        .cidrBlock("10.0.1.0/24")
                        .availabilityZone(FLOCI_REGION + "a")
                        .tagSpecifications(nameTag(ResourceType.SUBNET)))
                .subnet()
                .subnetId();
        String securityGroupId = ec2.createSecurityGroup(request -> request
                        .vpcId(vpcId)
                        .groupName(NAME)
                        .description("Trino test security group"))
                .groupId();
        ec2.createKeyPair(request -> request.keyName(NAME));
        ec2.createLaunchTemplate(request -> request
                .launchTemplateName(NAME)
                .launchTemplateData(data -> data
                        .imageId(IMAGE_ID)
                        .instanceType(INSTANCE_TYPE)));
        ec2.createVpcEndpoint(request -> request
                .vpcId(vpcId)
                .serviceName("com.amazonaws.%s.s3".formatted(FLOCI_REGION)));
        List<String> instanceIds = ec2.runInstances(request -> request
                        .imageId(IMAGE_ID)
                        .instanceType(InstanceType.fromValue(INSTANCE_TYPE))
                        .minCount(2)
                        .maxCount(2)
                        .subnetId(subnetId)
                        .securityGroupIds(securityGroupId)
                        .tagSpecifications(nameTag(ResourceType.INSTANCE)))
                .instances()
                .stream()
                .map(Instance::instanceId)
                .toList();
        return new EmulatorTestData(vpcId, subnetId, securityGroupId, instanceIds);
    }

    private static TagSpecification nameTag(ResourceType resourceType)
    {
        return TagSpecification.builder()
                .resourceType(resourceType)
                .tags(Tag.builder().key("Name").value(NAME).build())
                .build();
    }

    private static void createBuckets(S3Client s3)
    {
        s3.createBucket(request -> request.bucket(OBJECTS_BUCKET));
        s3.putObject(request -> request.bucket(OBJECTS_BUCKET).key("a.txt"), RequestBody.fromString("hello"));
        s3.putObject(request -> request.bucket(OBJECTS_BUCKET).key("dir/b.txt"), RequestBody.fromString("hello world"));

        s3.createBucket(request -> request.bucket(VERSIONED_BUCKET));
        s3.putBucketVersioning(request -> request
                .bucket(VERSIONED_BUCKET)
                .versioningConfiguration(configuration -> configuration.status(BucketVersioningStatus.ENABLED)));
        s3.putObject(request -> request.bucket(VERSIONED_BUCKET).key("changed.txt"), RequestBody.fromString("v1"));
        s3.putObject(request -> request.bucket(VERSIONED_BUCKET).key("changed.txt"), RequestBody.fromString("v2-longer"));
        s3.putObject(request -> request.bucket(VERSIONED_BUCKET).key("deleted.txt"), RequestBody.fromString("gone"));
        s3.deleteObject(request -> request.bucket(VERSIONED_BUCKET).key("deleted.txt"));

        createManyObjects(s3);
    }

    // S3 returns at most 1000 keys, versions or delete markers in one response
    private static void createManyObjects(S3Client s3)
    {
        s3.createBucket(request -> request.bucket(MANY_OBJECTS_BUCKET));
        s3.putBucketVersioning(request -> request
                .bucket(MANY_OBJECTS_BUCKET)
                .versioningConfiguration(configuration -> configuration.status(BucketVersioningStatus.ENABLED)));
        List<String> keys = IntStream.range(0, MANY_OBJECTS_LIVE_COUNT + MANY_OBJECTS_DELETED_COUNT)
                .mapToObj("key-%05d"::formatted)
                .toList();
        try (ExecutorService executor = Executors.newFixedThreadPool(16)) {
            List<Future<?>> uploads = keys.stream()
                    .<Future<?>>map(key -> executor.submit(() -> s3.putObject(request -> request.bucket(MANY_OBJECTS_BUCKET).key(key), RequestBody.fromString(key))))
                    .toList();
            uploads.forEach(Futures::getUnchecked);
        }
        for (List<String> batch : Lists.partition(keys.subList(MANY_OBJECTS_LIVE_COUNT, keys.size()), 1000)) {
            DeleteObjectsResponse response = s3.deleteObjects(request -> request
                    .bucket(MANY_OBJECTS_BUCKET)
                    .delete(delete -> delete.objects(batch.stream()
                            .map(key -> ObjectIdentifier.builder().key(key).build())
                            .toList())));
            checkState(response.errors().isEmpty(), "Failed to delete objects: %s", response.errors());
        }
    }
}
