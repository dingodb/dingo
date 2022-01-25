/*
 * Copyright 2021 DataCanvas
 *
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

package io.dingodb.raft.rpc;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;
import io.dingodb.raft.error.MessageClassNotFoundException;
import io.dingodb.raft.storage.io.ProtoBufFile;
import io.dingodb.raft.util.RpcFactoryHelper;
import org.apache.commons.lang.SerializationException;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.invoke.MethodType.methodType;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class ProtobufMsgFactory {
    private static Map<String/* class name in proto file */, MethodHandle> PARSE_METHODS_4PROTO = new HashMap<>();
    private static Map<String/* class name in java file */, MethodHandle>  PARSE_METHODS_4J = new HashMap<>();
    private static Map<String/* class name in java file */, MethodHandle>  DEFAULT_INSTANCE_METHODS_4J = new HashMap<>();

    static {
        try {
            final FileDescriptorSet descriptorSet = FileDescriptorSet.parseFrom(ProtoBufFile.class
                .getResourceAsStream("/raft.desc"));
            final List<FileDescriptor> resolveFDs = new ArrayList<>();
            final RaftRpcFactory rpcFactory = RpcFactoryHelper.rpcFactory();
            for (final FileDescriptorProto fdp : descriptorSet.getFileList()) {

                final FileDescriptor[] dependencies = new FileDescriptor[resolveFDs.size()];
                resolveFDs.toArray(dependencies);

                final FileDescriptor fd = FileDescriptor.buildFrom(fdp, dependencies);
                resolveFDs.add(fd);
                for (final Descriptor descriptor : fd.getMessageTypes()) {

                    final String className = fdp.getOptions().getJavaPackage() + "."
                                             + fdp.getOptions().getJavaOuterClassname() + "$" + descriptor.getName();
                    final Class<?> clazz = Class.forName(className);
                    final MethodHandle parseFromHandler = MethodHandles.lookup().findStatic(clazz, "parseFrom",
                        methodType(clazz, byte[].class));
                    final MethodHandle getInstanceHandler = MethodHandles.lookup().findStatic(clazz,
                        "getDefaultInstance", methodType(clazz));
                    PARSE_METHODS_4PROTO.put(descriptor.getFullName(), parseFromHandler);
                    PARSE_METHODS_4J.put(className, parseFromHandler);
                    DEFAULT_INSTANCE_METHODS_4J.put(className, getInstanceHandler);
                    rpcFactory.registerProtobufSerializer(className, getInstanceHandler.invoke());
                }

            }
        } catch (final Throwable t) {
            t.printStackTrace(); // NOPMD
        }
    }

    public static void load() {
        if (PARSE_METHODS_4J.isEmpty() || PARSE_METHODS_4PROTO.isEmpty() || DEFAULT_INSTANCE_METHODS_4J.isEmpty()) {
            throw new IllegalStateException("Parse protocol file failed.");
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends Message> T getDefaultInstance(final String className) {
        final MethodHandle handle = DEFAULT_INSTANCE_METHODS_4J.get(className);
        if (handle == null) {
            throw new MessageClassNotFoundException(className + " not found");
        }
        try {
            return (T) handle.invoke();
        } catch (Throwable t) {
            throw new SerializationException(t);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends Message> T newMessageByJavaClassName(final String className, final byte[] bs) {
        final MethodHandle handle = PARSE_METHODS_4J.get(className);
        if (handle == null) {
            throw new MessageClassNotFoundException(className + " not found");
        }
        try {
            return (T) handle.invoke(bs);
        } catch (Throwable t) {
            throw new SerializationException(t);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends Message> T newMessageByProtoClassName(final String className, final byte[] bs) {
        final MethodHandle handle = PARSE_METHODS_4PROTO.get(className);
        if (handle == null) {
            throw new MessageClassNotFoundException(className + " not found");
        }
        try {
            return (T) handle.invoke(bs);
        } catch (Throwable t) {
            throw new SerializationException(t);
        }
    }
}
