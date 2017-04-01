package org.cloudbus.cloudsim.resources;

/**
 * A class that implements the Null Object Design Pattern for
 * {@link ResourceManageable} class.
 *
 * @author Manoel Campos da Silva Filho
 * @see ResourceManageable#NULL
 */
final class ResourceManageableNull implements ResourceManageable {
    @Override public boolean setCapacity(long newCapacity) {
        return false;
    }
    @Override public boolean allocateResource(long amountToAllocate) {
        return false;
    }
    @Override public boolean setAllocatedResource(long newTotalAllocatedResource) {
        return false;
    }
    @Override public boolean deallocateResource(long amountToDeallocate) {
        return false;
    }
    @Override public long deallocateAllResources() {
        return 0L;
    }
    @Override public boolean isResourceAmountBeingUsed(long amountToCheck) {
        return false;
    }
    @Override public boolean isSuitable(long newTotalAllocatedResource) {
        return false;
    }
    @Override public long getCapacity() {
        return 0L;
    }
    @Override public long getAvailableResource() {
        return 0L;
    }
    @Override public long getAllocatedResource() {
        return 0L;
    }
    @Override public boolean isResourceAmountAvailable(long amountToCheck) {
        return false;
    }
    @Override public boolean isResourceAmountAvailable(double amountToCheck) {
        return false;
    }
    @Override public boolean isFull() {
        return false;
    }
}
