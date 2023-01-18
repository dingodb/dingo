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

package io.dingodb.strorage.column;

import java.io.IOException;

public class Environment {
  private static String OS = System.getProperty("os.name").toLowerCase();
  private static String ARCH = System.getProperty("os.arch").toLowerCase();
  private static boolean MUSL_LIBC;

  static {
    try {
      final Process p = new ProcessBuilder("/usr/bin/env", "sh", "-c", "ldd /usr/bin/env | grep -q musl").start();
      MUSL_LIBC = p.waitFor() == 0;
    } catch (final IOException | InterruptedException e) {
      MUSL_LIBC = false;
    }
  }

  public static boolean isAarch64() {
    return ARCH.contains("aarch64");
  }

  public static boolean isPowerPC() {
    return ARCH.contains("ppc");
  }

  public static boolean isS390x() {
    return ARCH.contains("s390x");
  }

  public static boolean isWindows() {
    return (OS.contains("win"));
  }

  public static boolean isFreeBSD() {
    return (OS.contains("freebsd"));
  }

  public static boolean isMac() {
    return (OS.contains("mac"));
  }

  public static boolean isAix() {
    return OS.contains("aix");
  }
  
  public static boolean isUnix() {
    return OS.contains("nix") ||
        OS.contains("nux");
  }

  public static boolean isMuslLibc() {
    return MUSL_LIBC;
  }

  public static boolean isSolaris() {
    return OS.contains("sunos");
  }

  public static boolean isOpenBSD() {
    return (OS.contains("openbsd"));
  }

  public static boolean is64Bit() {
    if (ARCH.indexOf("sparcv9") >= 0) {
      return true;
    }
    return (ARCH.indexOf("64") > 0);
  }

  public static String getSharedLibraryName(final String name) {
    return name + "jni";
  }

  public static String getSharedLibraryFileName(final String name) {
    return appendLibOsSuffix("lib" + getSharedLibraryName(name), true);
  }

  /**
   * Get the name of the libc implementation
   *
   * @return the name of the implementation,
   *    or null if the default for that platform (e.g. glibc on Linux).
   */
  public static /* @Nullable */ String getLibcName() {
    if (isMuslLibc()) {
      return "musl";
    } else {
      return null;
    }
  }

  private static String getLibcPostfix() {
    final String libcName = getLibcName();
    if (libcName == null) {
      return "";
    }
    return "-" + libcName;
  }

  public static String getJniLibraryName(final String name) {
    if (isUnix()) {
      final String arch = is64Bit() ? "64" : "32";
      if (isPowerPC() || isAarch64()) {
        return String.format("%sjni-linux-%s%s", name, ARCH, getLibcPostfix());
      } else if (isS390x()) {
        return String.format("%sjni-linux-%s", name, ARCH);
      } else {
        return String.format("%sjni-linux%s%s", name, arch, getLibcPostfix());
      }
    } else if (isMac()) {
      if (is64Bit()) {
        final String arch;
        if (isAarch64()) {
          arch = "arm64";
        } else {
          arch = "x86_64";
        }
        return String.format("%sjni-osx-%s", name, arch);
      } else {
        return String.format("%sjni-osx", name);
      }
    } else if (isFreeBSD()) {
      return String.format("%sjni-freebsd%s", name, is64Bit() ? "64" : "32");
    } else if (isAix() && is64Bit()) {
      return String.format("%sjni-aix64", name);
    } else if (isSolaris()) {
      final String arch = is64Bit() ? "64" : "32";
      return String.format("%sjni-solaris%s", name, arch);
    } else if (isWindows() && is64Bit()) {
      return String.format("%sjni-win64", name);
    } else if (isOpenBSD()) {
      return String.format("%sjni-openbsd%s", name, is64Bit() ? "64" : "32");
    }

    throw new UnsupportedOperationException(String.format("Cannot determine JNI library name for ARCH='%s' OS='%s' name='%s'", ARCH, OS, name));
  }

  public static /*@Nullable*/ String getFallbackJniLibraryName(final String name) {
    if (isMac() && is64Bit()) {
      return String.format("%sjni-osx", name);
    }
    return null;
  }

  public static String getJniLibraryFileName(final String name) {
    return appendLibOsSuffix("lib" + getJniLibraryName(name), false);
  }

  public static /*@Nullable*/ String getFallbackJniLibraryFileName(final String name) {
    final String fallbackJniLibraryName = getFallbackJniLibraryName(name);
    if (fallbackJniLibraryName == null) {
      return null;
    }
    return appendLibOsSuffix("lib" + fallbackJniLibraryName, false);
  }

  private static String appendLibOsSuffix(final String libraryFileName, final boolean shared) {
    if (isUnix() || isAix() || isSolaris() || isFreeBSD() || isOpenBSD()) {
      return libraryFileName + ".so";
    } else if (isMac()) {
      return libraryFileName + (shared ? ".dylib" : ".jnilib");
    } else if (isWindows()) {
      return libraryFileName + ".dll";
    }
    throw new UnsupportedOperationException();
  }

  public static String getJniLibraryExtension() {
    if (isWindows()) {
      return ".dll";
    }
    return (isMac()) ? ".jnilib" : ".so";
  }
}
