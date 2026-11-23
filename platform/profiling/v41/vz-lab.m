#import <CommonCrypto/CommonDigest.h>
#import <Foundation/Foundation.h>
#import <Virtualization/Virtualization.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <unistd.h>

static const uint32_t resultPort = 29617;
static const uint64_t resultLimit = 256ULL << 20;
static const char resultMagic[] = "AHPV41R1";
enum { magicLength = sizeof(resultMagic) - 1 };

static BOOL writeAll(int fd, const void *data, size_t count) {
    const char *next = data;
    while (count) {
        ssize_t n = write(fd, next, count);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return NO;
        next += n;
        count -= n;
    }
    return YES;
}

static BOOL readAll(int fd, void *data, size_t count) {
    char *next = data;
    while (count) {
        ssize_t n = read(fd, next, count);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return NO;
        next += n;
        count -= n;
    }
    return YES;
}

static uint64_t available(const char *path) {
    struct statfs info;
    if (statfs(path, &info) != 0) return 0;
    return (uint64_t)info.f_bavail * info.f_bsize;
}

// Read an exact payload length. The vsock peer remains open for the acknowledgment.
static const char *receiveArchive(int fd, NSString *path, uint64_t reserve, uint64_t *received) {
    struct timeval timeout = {.tv_sec = 90};
    int flags = fcntl(fd, F_GETFL);
    if (flags < 0 || fcntl(fd, F_SETFL, flags & ~O_NONBLOCK) != 0 ||
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0 ||
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) != 0) {
        return "socket";
    }

    enum { digestOffset = magicLength + sizeof(uint64_t) };
    unsigned char header[digestOffset + CC_SHA256_DIGEST_LENGTH];
    if (!readAll(fd, header, sizeof(header))) return "header_read";
    if (memcmp(header, resultMagic, magicLength) != 0) return "header_magic";
    uint64_t wireLength;
    memcpy(&wireLength, header + magicLength, sizeof(wireLength));
    uint64_t remaining = CFSwapInt64BigToHost(wireLength);
    if (remaining == 0 || remaining > resultLimit) return "size_limit";

    NSString *partial = [path stringByAppendingString:@".partial"];
    int out = open(partial.fileSystemRepresentation, O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (out < 0) return "archive_open";
    const char *failure = NULL;
    CC_SHA256_CTX digest;
    CC_SHA256_Init(&digest);
    char buffer[64 * 1024];
    while (remaining > 0) {
        size_t count = remaining < sizeof(buffer) ? (size_t)remaining : sizeof(buffer);
        if (!readAll(fd, buffer, count)) {
            failure = "payload_read";
            break;
        }
        if (available(partial.fileSystemRepresentation) <= reserve || !writeAll(out, buffer, count)) {
            failure = "archive_write";
            break;
        }
        CC_SHA256_Update(&digest, buffer, (CC_LONG)count);
        remaining -= count;
        *received += count;
    }
    unsigned char hash[CC_SHA256_DIGEST_LENGTH];
    CC_SHA256_Final(hash, &digest);
    if (!failure && memcmp(hash, header + digestOffset, sizeof(hash)) != 0) failure = "digest_mismatch";
    if (fsync(out) != 0 && !failure) failure = "archive_sync";
    if (close(out) != 0 && !failure) failure = "archive_close";
    if (!failure && rename(partial.fileSystemRepresentation, path.fileSystemRepresentation) != 0) {
        failure = "archive_rename";
    }
    return failure;
}

@interface Results : NSObject <VZVirtioSocketListenerDelegate>
@property (nonatomic, copy) NSString *path;
@property (nonatomic) uint64_t reserve;
@property (nonatomic) BOOL active;
@end

@implementation Results
- (BOOL)listener:(VZVirtioSocketListener *)listener
    shouldAcceptNewConnection:(VZVirtioSocketConnection *)connection
              fromSocketDevice:(VZVirtioSocketDevice *)device {
    if (self.active) return NO;
    self.active = YES;
    dispatch_async(dispatch_get_global_queue(QOS_CLASS_UTILITY, 0), ^{
        int fd = connection.fileDescriptor;
        uint64_t received = 0;
        const char *failure = receiveArchive(fd, self.path, self.reserve, &received);
        const char ack = failure ? '0' : '1';
        if (!writeAll(fd, &ack, 1) && !failure) failure = "ack_write";
        fprintf(stderr, "AHP_V41_VZ_RESULTS status=%s bytes=%llu reason=%s\n",
            failure ? "failed" : "ok", received, failure ? failure : "verified");
        (void)connection.fileDescriptor;
    });
    return YES;
}
@end

@interface Lifecycle : NSObject <VZVirtualMachineDelegate>
@end
@implementation Lifecycle
- (void)guestDidStopVirtualMachine:(VZVirtualMachine *)machine {
    fputs("AHP_V41_VZ_GUEST_STOPPED\n", stderr);
    exit(0);
}
- (void)virtualMachine:(VZVirtualMachine *)machine didStopWithError:(NSError *)error {
    fprintf(stderr, "AHP_V41_VZ_ERROR %s\n", error.localizedDescription.UTF8String);
    exit(2);
}
@end

int main(int argc, char **argv) {
    if (argc != 8) {
        fputs("usage: vz-lab kernel initramfs data.img results.tar.gz vcpu memory-mib timeout-seconds\n", stderr);
        return 64;
    }
    signal(SIGPIPE, SIG_IGN);
    if (![VZGenericPlatformConfiguration isNestedVirtualizationSupported]) {
        fputs("AHP_V41_VZ_NESTED_UNAVAILABLE\n", stderr);
        return 65;
    }
    NSError *error = nil;
    NSURL *diskURL = [NSURL fileURLWithPath:@(argv[3])];
    VZDiskImageStorageDeviceAttachment *disk = [[VZDiskImageStorageDeviceAttachment alloc]
        initWithURL:diskURL readOnly:NO cachingMode:VZDiskImageCachingModeUncached
        synchronizationMode:VZDiskImageSynchronizationModeFull error:&error];
    if (!disk) {
        fprintf(stderr, "AHP_V41_VZ_DISK %s\n", error.localizedDescription.UTF8String);
        return 66;
    }

    VZGenericPlatformConfiguration *platform = [[VZGenericPlatformConfiguration alloc] init];
    platform.nestedVirtualizationEnabled = YES;
    VZLinuxBootLoader *boot = [[VZLinuxBootLoader alloc] initWithKernelURL:[NSURL fileURLWithPath:@(argv[1])]];
    boot.initialRamdiskURL = [NSURL fileURLWithPath:@(argv[2])];
    boot.commandLine = @"console=hvc0 rdinit=/init panic=8 quiet loglevel=5";

    VZVirtualMachineConfiguration *config = [VZVirtualMachineConfiguration new];
    config.platform = platform;
    config.bootLoader = boot;
    config.CPUCount = strtoul(argv[5], NULL, 10);
    config.memorySize = strtoull(argv[6], NULL, 10) << 20;
    config.storageDevices = @[[[VZVirtioBlockDeviceConfiguration alloc] initWithAttachment:disk]];
    config.socketDevices = @[[[VZVirtioSocketDeviceConfiguration alloc] init]];
    config.entropyDevices = @[[[VZVirtioEntropyDeviceConfiguration alloc] init]];
    VZVirtioConsoleDeviceSerialPortConfiguration *serial = [[VZVirtioConsoleDeviceSerialPortConfiguration alloc] init];
    serial.attachment = [[VZFileHandleSerialPortAttachment alloc]
        initWithFileHandleForReading:nil fileHandleForWriting:NSFileHandle.fileHandleWithStandardOutput];
    config.serialPorts = @[serial];
    if (![config validateWithError:&error]) {
        fprintf(stderr, "AHP_V41_VZ_CONFIG %s\n", error.localizedDescription.UTF8String);
        return 66;
    }

    VZVirtualMachine *machine = [[VZVirtualMachine alloc] initWithConfiguration:config];
    __attribute__((objc_precise_lifetime)) Lifecycle *lifecycle = [Lifecycle new];
    __attribute__((objc_precise_lifetime)) Results *results = [Results new];
    machine.delegate = lifecycle;
    results.path = @(argv[4]);
    results.reserve = 512ULL << 20;
    VZVirtioSocketListener *listener = [VZVirtioSocketListener new];
    listener.delegate = results;
    VZVirtioSocketDevice *socketDevice = (VZVirtioSocketDevice *)machine.socketDevices.firstObject;
    [socketDevice setSocketListener:listener forPort:resultPort];

    [machine startWithCompletionHandler:^(NSError *startError) {
        if (startError) {
            fprintf(stderr, "AHP_V41_VZ_START %s\n", startError.localizedDescription.UTF8String);
            exit(67);
        }
        fputs("AHP_V41_VZ_STARTED\n", stderr);
    }];
    uint64_t timeout = strtoull(argv[7], NULL, 10);
    dispatch_after(dispatch_time(DISPATCH_TIME_NOW, timeout * NSEC_PER_SEC), dispatch_get_main_queue(), ^{
        fputs("AHP_V41_VZ_TIMEOUT\n", stderr);
        [machine stopWithCompletionHandler:^(NSError *stopError) { exit(124); }];
    });
    dispatch_source_t spaceCheck = dispatch_source_create(DISPATCH_SOURCE_TYPE_TIMER, 0, 0, dispatch_get_main_queue());
    dispatch_source_set_timer(spaceCheck, dispatch_time(DISPATCH_TIME_NOW, NSEC_PER_SEC), NSEC_PER_SEC, NSEC_PER_SEC / 4);
    dispatch_source_set_event_handler(spaceCheck, ^{
        if (available(diskURL.fileSystemRepresentation) < results.reserve) {
            fputs("AHP_V41_VZ_HOST_SPACE_FLOOR\n", stderr);
            dispatch_source_cancel(spaceCheck);
            [machine stopWithCompletionHandler:^(NSError *stopError) { exit(74); }];
        }
    });
    dispatch_resume(spaceCheck);
    [[NSRunLoop currentRunLoop] run];
    return 68;
}
