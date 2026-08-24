package com.maximus.runner.application.monitoring.collector;

import com.maximus.runner.HealthStatus;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class SystemHealthCollector {

    private static final DateTimeFormatter ISO_FORMATTER =
            DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    public HealthStatus collect() {
        OperatingSystemMXBean operatingSystem =
                ManagementFactory.getOperatingSystemMXBean();
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();

        HealthStatus.Builder healthStatus = HealthStatus.newBuilder()
                .setServer(buildServer(operatingSystem))
                .setCpu(buildCpu(operatingSystem))
                .setMemory(buildMemory(operatingSystem, memoryBean))
                .addAllDisks(buildDisks())
                .setNetwork(buildNetwork())
                .setLoad(buildLoad(operatingSystem))
                .setProcess(buildProcess());

        return healthStatus.build();
    }

    private HealthStatus.Server buildServer(OperatingSystemMXBean operatingSystem) {
        HealthStatus.Server.Builder server = HealthStatus.Server.newBuilder()
                .setOperatingSystem(System.getProperty("os.name", "unknown"))
                .setOperatingSystemVersion(System.getProperty("os.version", "unknown"))
                .setArchitecture(System.getProperty("os.arch", "unknown"))
                .setLogicalProcessors(operatingSystem.getAvailableProcessors())
                .setUptimeSeconds(ManagementFactory.getRuntimeMXBean().getUptime() / 1000)
                .setCurrentTime(ISO_FORMATTER.format(Instant.now().atZone(ZoneId.systemDefault())))
                .setTimezone(ZoneId.systemDefault().getId());

        try {
            server.setHostname(InetAddress.getLocalHost().getHostName());
        } catch (Exception exception) {
            server.setHostname("unknown");
        }

        if (operatingSystem instanceof com.sun.management.OperatingSystemMXBean sunOs) {
            int physicalCores = sunOs.getAvailableProcessors();
            server.setPhysicalCores(physicalCores);
        }

        return server.build();
    }

    private HealthStatus.Cpu buildCpu(OperatingSystemMXBean operatingSystem) {
        HealthStatus.Cpu.Builder cpu = HealthStatus.Cpu.newBuilder()
                .setLogicalProcessors(operatingSystem.getAvailableProcessors());

        double loadAverage = operatingSystem.getSystemLoadAverage();
        if (loadAverage >= 0) {
            cpu.setLoadAverage(loadAverage);
        }

        if (operatingSystem instanceof com.sun.management.OperatingSystemMXBean sunOs) {
            double systemCpuLoad = sunOs.getCpuLoad();
            if (systemCpuLoad >= 0) {
                cpu.setUsagePercentage(systemCpuLoad * 100.0);
            }
        }

        return cpu.build();
    }

    private HealthStatus.Memory buildMemory(
            OperatingSystemMXBean operatingSystem,
            MemoryMXBean memoryBean
    ) {
        HealthStatus.Memory.Builder memory = HealthStatus.Memory.newBuilder();

        if (operatingSystem instanceof com.sun.management.OperatingSystemMXBean sunOs) {
            long totalBytes = sunOs.getTotalMemorySize();
            long freeBytes = sunOs.getFreeMemorySize();

            if (totalBytes > 0) {
                long usedBytes = totalBytes - freeBytes;
                memory.setTotalBytes(totalBytes);
                memory.setAvailableBytes(freeBytes);
                memory.setUsedBytes(usedBytes);
                memory.setUsagePercentage((usedBytes * 100.0) / totalBytes);
            }

            long swapTotal = sunOs.getTotalSwapSpaceSize();
            long swapFree = sunOs.getFreeSwapSpaceSize();
            if (swapTotal > 0) {
                long swapUsed = swapTotal - swapFree;
                memory.setSwapTotalBytes(swapTotal);
                memory.setSwapAvailableBytes(swapFree);
                memory.setSwapUsedBytes(swapUsed);
                memory.setSwapUsagePercentage((swapUsed * 100.0) / swapTotal);
            }
        } else {
            long heapUsed = memoryBean.getHeapMemoryUsage().getUsed();
            long heapMax = memoryBean.getHeapMemoryUsage().getMax();
            memory.setUsedBytes(heapUsed);
            memory.setTotalBytes(heapMax > 0 ? heapMax : heapUsed);
            memory.setAvailableBytes(Math.max(0, heapMax - heapUsed));
            if (heapMax > 0) {
                memory.setUsagePercentage((heapUsed * 100.0) / heapMax);
            }
        }

        return memory.build();
    }

    private List<HealthStatus.Disk> buildDisks() {
        List<HealthStatus.Disk> disks = new ArrayList<>();

        for (File root : File.listRoots()) {
            long totalBytes = root.getTotalSpace();
            if (totalBytes <= 0) {
                continue;
            }

            long freeBytes = root.getFreeSpace();
            long usedBytes = totalBytes - freeBytes;

            disks.add(
                    HealthStatus.Disk.newBuilder()
                            .setDevice(root.getAbsolutePath())
                            .setMountPoint(root.getAbsolutePath())
                            .setTotalBytes(totalBytes)
                            .setFreeBytes(freeBytes)
                            .setUsedBytes(usedBytes)
                            .setUsagePercentage((usedBytes * 100.0) / totalBytes)
                            .setReadOnly(!root.canWrite())
                            .build()
            );
        }

        return disks;
    }

    private HealthStatus.Network buildNetwork() {
        HealthStatus.Network.Builder network = HealthStatus.Network.newBuilder();
        List<HealthStatus.NetworkInterface> interfaces = new ArrayList<>();

        int activeInterfaces = 0;

        try {
            Enumeration<NetworkInterface> networkInterfaces =
                    NetworkInterface.getNetworkInterfaces();

            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();

                if (networkInterface.isLoopback()) {
                    continue;
                }

                HealthStatus.NetworkInterface.Builder interfaceBuilder =
                        HealthStatus.NetworkInterface.newBuilder()
                                .setName(networkInterface.getName())
                                .setActive(networkInterface.isUp());

                byte[] hardwareAddress = networkInterface.getHardwareAddress();
                if (hardwareAddress != null) {
                    interfaceBuilder.setMacAddress(formatMacAddress(hardwareAddress));
                }

                Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    interfaceBuilder.addAddresses(addresses.nextElement().getHostAddress());
                }

                if (networkInterface.isUp()) {
                    activeInterfaces++;
                }

                interfaces.add(interfaceBuilder.build());
            }
        } catch (Exception exception) {
            // Return partial network data.
        }

        network.setInterfaceCount(interfaces.size());
        network.setActiveInterfaces(activeInterfaces);
        network.addAllInterfaces(interfaces);

        return network.build();
    }

    private HealthStatus.Load buildLoad(OperatingSystemMXBean operatingSystem) {
        HealthStatus.Load.Builder load = HealthStatus.Load.newBuilder();

        double systemLoadAverage = operatingSystem.getSystemLoadAverage();
        if (systemLoadAverage >= 0) {
            load.setLoad1Minute(systemLoadAverage);
        }

        return load.build();
    }

    private HealthStatus.Process buildProcess() {
        Runtime runtime = Runtime.getRuntime();
        long processCount = ProcessHandle.allProcesses().count();

        return HealthStatus.Process.newBuilder()
                .setProcessCount((int) Math.min(processCount, Integer.MAX_VALUE))
                .setThreadCount(Thread.activeCount())
                .setTotalMemoryBytes(runtime.totalMemory() - runtime.freeMemory())
                .build();
    }

    private String formatMacAddress(byte[] hardwareAddress) {
        StringBuilder builder = new StringBuilder();

        for (int index = 0; index < hardwareAddress.length; index++) {
            if (index > 0) {
                builder.append(':');
            }
            builder.append(String.format("%02X", hardwareAddress[index]));
        }

        return builder.toString();
    }
}
