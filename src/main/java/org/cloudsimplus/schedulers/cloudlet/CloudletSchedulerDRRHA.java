package org.cloudsimplus.schedulers.cloudlet;



import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletExecution;
import org.cloudsimplus.schedulers.MipsShare;
import org.cloudsimplus.util.MathUtil;

import java.io.Serial;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Double.NaN;
import static java.util.stream.Collectors.toList;
/*
    DRRHA Algorithm
    1 Arrange submitted tasks in ascending order based on burst time
    2 Compute arithmetic mean (m) of all tasks in ready queue
    3 Time quanta, T(i,j) = (m/2) + (m/2)/ B(i,j) where B is burst time of ith cloudlet in round j
    4 (i) Remaining burst time < Time quanta then cloudlet is complete execution
      (ii) Remaining burst time > time quanta then cloudlet is put in the tail end of waiting list
    5 When new tasks arrives the whole process is repeated
    6 When task is finished m and T(i,j) is recalculated
    7 This continues till all tasks are finished
 */
//TO DO

public final class CloudletSchedulerDRRHA extends CloudletSchedulerTimeShared{


    @Serial
    private static final long serialVersionUID = 9077807080813489474L;

    private int minimumGranularity = 2;
    public int contextSwitches = 0;

    /**
     * A comparator used to increasingly sort Cloudlets into the waiting list
     * based on their virtual runtime (vruntime or VRT). By this way, the Cloudlets in the beginning
     * of such a list will be that ones which have run the least and have to be
     * prioritized when getting Cloudlets from this list to add to the execution
     * list.
     *
     * @param c1 first Cloudlet to compare
     * @param c2 second Cloudlet to compare
     * @return a negative value if c1 is lower than c2, zero if they are equals,
     * a positive value if c1 is greater than c2
     */
    private int waitingCloudletsComparator(final CloudletExecution c1, final CloudletExecution c2){
        System.out.println("[IN] : DRRHA > waitingCloudletsComparator");
        final double vRuntimeDiff = getRemainingBurstTime(c1) - getRemainingBurstTime(c2);
        if (vRuntimeDiff != 0) {
            return MathUtil.doubleToInt(vRuntimeDiff);
        }


        final long idDiff = c1.getCloudletId() - c2.getCloudletId();
        //Since the computed value is long but the comparator return must be int, rounds the value to the closest int
        return Math.round( idDiff);
    }

    /**
     * Calculates the mean of burst time of the current run queue
     */
    private double getMeanBurstTime(){
        double mean = 0.0;
        if(getCloudletWaitingList().size() == 0) return mean;
        for(CloudletExecution c: getCloudletWaitingList()){
            mean+=getRemainingBurstTime(c);
        }
        mean/=getCloudletWaitingList().size();
        return mean;
    }

    /**
     * Calculate the mean of burst time of run queue including the burst time of the cloudlet
     * being passed to the function
     */
    private double getMeanBurstTime(CloudletExecution c){
        double mean = 0.0;
        int sze = getCloudletWaitingList().size();
        mean = getMeanBurstTime();
        if(mean==0.0) return getRemainingBurstTime(c);
        mean+=getRemainingBurstTime(c);
        mean/=(sze+1);
        return mean;
    }

    /**
     * Calculates the remaining burst time of a cloudlet based on its
     * remaining length, number of PES used by cloudlet,
     * MIPS of a PES
     */
    private double getRemainingBurstTime(CloudletExecution c){
        double burstTime = 0.0;
        burstTime+= (c.getRemainingCloudletLength()/getAvailableMipsByPe())*(c.getPesNumber());
        return burstTime;
    }


    /**
     * Function to check if remaining burst time < calculated time slice for a cloudlet
     */
    private boolean checkCondition (CloudletExecution c){
        double timeslice = computeCloudletTimeSlice(c);
        double burstTime = getRemainingBurstTime(c);
        return timeslice>=burstTime;
    }


    /**
     * Increments when there is a context switch involved
     */

    private void incrementContextSwitch(){
        contextSwitches+=1;
    }

    /**
     * Increments context switch by given ammount
     */
    private void incrementContextSwitch(int x){
        contextSwitches+=x;
    }

    /**
     * Returns the total no of context switches that has been carried out
     */
    public int getContextSwitches(){
        return contextSwitches;
    }

    /**
     * Computes the time-slice for a Cloudlet, which is the amount
     * of time (in seconds) that it will have to use the PEs,
     * considering all Cloudlets in the {@link #getCloudletWaitingList()} waiting list}.
     *
     * The timeslice is computed considering the DRRHA Algorithm i .e.
     * time slice = mean/2 + (mean/2)/remaining_burst_time
     *
     * @param cloudlet Cloudlet to get the time-slice
     * @return Cloudlet time-slice (in seconds)
     *
     */
    private double computeCloudletTimeSlice(final CloudletExecution cloudlet){
        double timeSlice = 0.0;
        if(findCloudletInList(cloudlet.getCloudlet(),getCloudletWaitingList()).isPresent()){
            timeSlice  = getMeanBurstTime()/2 + (getMeanBurstTime()/2)/getRemainingBurstTime(cloudlet);
        }

        else{
            timeSlice = getMeanBurstTime(cloudlet)/2 + (getMeanBurstTime(cloudlet)/2)/getRemainingBurstTime(cloudlet);
        }
        return timeSlice;
    }

    /**
     * Gets a <b>read-only</b> list of Cloudlets which are waiting to run, the so called
     * <a href="https://en.wikipedia.org/wiki/Run_queue">run queue</a>.
     *
     * <p>
     * <b>NOTE:</b> Different from real implementations, this scheduler uses just one run queue
     * for all processor cores (PEs). Since CPU context switch is not concerned,
     * there is no point in using different run queues.
     * </p>
     *
     * @return
     */
    @Override
    public List<CloudletExecution> getCloudletWaitingList() {
        return super.getCloudletWaitingList();
    }

    /**
     * {@inheritDoc}
     * The cloudlet waiting list (runqueue) is sorted according to the remaining Burst Time (vruntime or VRT),
     * which indicates the amount of time the Cloudlet has yet to till completion .
     * This runtime increases as the Cloudlet executes.
     *
     * @return {@inheritDoc}
     */
    @Override
    protected Optional<CloudletExecution> findSuitableWaitingCloudlet() {
        System.out.println("[IN] : DRRHA > findSuitableWaitingCloudlet");
        sortCloudletWaitingList(this::waitingCloudletsComparator);
        Optional<CloudletExecution> c = super.findSuitableWaitingCloudlet();
        System.out.println("[IN] : DRRHA > findSuitableWaitingCloudlet : Has found suitable cloudlet : "+c.toString());
        return c;
    }

    /**
     * Gets the minimum granularity that is the minimum amount of
     * time (in seconds) that is assigned to each
     * Cloudlet to execute.
     *
     * <p>This minimum value is used to reduce the frequency
     * of CPU context Datacenter, that degrade CPU throughput.
     * <b>However, CPU context switch overhead is not being considered.</b>
     * By this way, it just ensures that each Cloudlet will not use the CPU
     * for less than the minimum granularity.</p>
     *
     * <p>The default value for linux scheduler is 0.001s</p>
     *
     * @return
     *
     */
    public int getMinimumGranularity() {
        return minimumGranularity;
    }

    /**
     * Sets the minimum granularity that is the minimum amount of
     * time (in seconds) that is assigned to each
     * Cloudlet to execute.
     *
     * @param minimumGranularity the minimum granularity to set
     */
    public void setMinimumGranularity(final int minimumGranularity) {
        this.minimumGranularity = minimumGranularity;
    }



    //CHECK THIS FUNCTION

    /**
     * {@inheritDoc}
     *
     * <p>It also sets the initial virtual runtime for the given Cloudlet
     * in order to define how long the Cloudlet has executed yet.<br>
     *
     *
     * for more details.</p>
     *
     * @param cle {@inheritDoc}
     * @param fileTransferTime {@inheritDoc}
     * @return {@inheritDoc}
     */
    @Override
    protected double cloudletSubmitInternal(final CloudletExecution cle, final double fileTransferTime) {
        System.out.println("[IN]: DRRHA > cloudletSubmitInternal");
        cle.setVirtualRuntime(computeCloudletInitialVirtualRuntime(cle));
        cle.setTimeSlice(computeCloudletTimeSlice(cle));
        double ret = super.cloudletSubmitInternal(cle, fileTransferTime);
        System.out.println("[IN]: DRRHA > cloudletSubmitInternal : file transfer time + running time : "+ret);
        for(CloudletExecution c: getCloudletWaitingList()){
            System.out.println("[IN]: DRRHA > cloudletSubmitInternal : Run queue has "+c.getCloudletId());
        }
        return ret;
    }


    //CHECK THIS FUNCTION
    /**
     * {@inheritDoc}
     * @param currentTime {@inheritDoc}
     * @param mipsShare {@inheritDoc}
     * @return the shorter time-slice assigned to the running cloudlets (which defines
     * the time of the next expiring Cloudlet, enabling the preemption process), or Double.MAX_VALUE if there is no next events
     */
    @Override
    public double updateProcessing(final double currentTime, final MipsShare mipsShare) {
        System.out.println("[IN] : DRRHA > updateProcessing");
        super.updateProcessing(currentTime, mipsShare);

        return getCloudletExecList().stream()
            .mapToDouble(CloudletExecution::getTimeSlice)
            .min().orElse(Double.MAX_VALUE);
    }


    //CHECK THIS
    @Override
    public long updateCloudletProcessing(final CloudletExecution cle, final double currentTime) {
        System.out.println("[IN] : DRRHA > updateCloudletProcessing");
        /*
        Cloudlet has never been executed yet and it will start executing now,
        sets its actual virtual runtime. The negative value was used so far
        just to sort Cloudlets in the waiting list according to their priorities.
        */
        if(cle.getVirtualRuntime() < 0){
            cle.setVirtualRuntime(0);
        }


        //Check if program is already in the run queue or does it imply a context switch has to be carried out
        //if(getCloudletExecList().size()==0) incrementContextSwitch();

        //if(findCloudletInList(cle.getCloudlet(),getCloudletExecList()).isEmpty()) incrementContextSwitch();


        final double cloudletTimeSpan = currentTime - cle.getLastProcessingTime();
        final long partialFinishedMI = super.updateCloudletProcessing(cle, currentTime);

        cle.addVirtualRuntime(cloudletTimeSpan);
        System.out.println("[IN] : DRRHA > updateCloudletProcessing : partialFinishedMI after cloudlet processing is : "+partialFinishedMI);
        return partialFinishedMI;
    }

    //CHECK IF VIRTUAL RUNTIME IS NEEDED

    /**
     * Computes the initial virtual runtime for a Cloudlet that will be added to the execution list.
     * This virtual runtime is updated as long as the Cloudlet is executed.
     * The initial value is negative to indicate the Cloudlet hasn't started
     * executing yet. The virtual runtime is computed based on the Cloudlet priority.
     *
     * @param cloudlet Cloudlet to compute the initial virtual runtime
     * @return the computed initial virtual runtime as a negative value
     * to indicate that the Cloudlet hasn't started executing yet
     */
    private double computeCloudletInitialVirtualRuntime(final CloudletExecution cloudlet) {
        /*
        A negative virtual runtime indicates the cloudlet has never been executed yet.
        This math was used just to ensure that the first added cloudlets
        will have the lower vruntime, depending of their priorities.
        If all cloudlets have the same priority, the first
           added will start executing first.
        */

        /*
        Inverses the Cloudlet ID dividing the Integer.MAX_VALUE by it,
        because the ID is in fact int. This will make that the lower
        ID return higher values. It ensures that as lower is the ID,
        lower is the negative value returned.
        Inverting the cloudlet ID to get a higher value for lower IDs
        can be understood as resulting in "higher negative" values, that is,
        extreme negative values.
        */
        final double inverseOfCloudletId = Integer.MAX_VALUE/(cloudlet.getCloudletId()+1.0);

        return -Math.abs(getRemainingBurstTime(cloudlet) + inverseOfCloudletId);
    }


    /**
     * Checks if a Cloudlet can be submitted to the execution list.
     *
     * This scheduler, different from its time-shared parent, only adds
     * submitted Cloudlets to the execution list if there is enough free PEs.
     * Otherwise, such Cloudlets are added to the waiting list,
     * really enabling time-sharing between running Cloudlets.
     * By this way, some Cloudlets have to be preempted to allow other ones
     * to be executed.
     *
     * @param cloudlet {@inheritDoc}
     * @return {@inheritDoc}
     */
    @Override
    protected boolean canExecuteCloudletInternal(final CloudletExecution cloudlet) {
        System.out.println("[IN] : DRRHA > canExecuteCloudletInternal");
        return isThereEnoughFreePesForCloudlet(cloudlet) ;
        //&& checkCondition(cloudlet)
    }

    /**
     * {@inheritDoc}
     *
     * <p>Prior to start executing, a Cloudlet is added to this list.
     * When the Cloudlet vruntime reaches its time-slice (the amount of time
     * it can use the CPU), it is removed from this list and added
     * back to the {@link #getCloudletWaitingList()}.</p>
     *
     * <p>The sum of the PEs of Cloudlets into this list cannot exceeds
     * the number of PEs available for the scheduler. If the sum of PEs of such Cloudlets
     * is less than the number of existing PEs, there are
     * idle PEs. Since the CPU context switch overhead is not regarded
     * in this implementation and as result, it doesn't matter which
     * PEs are running which Cloudlets, there is not such information
     * in anywhere. As an example, if the first Cloudlet requires 2 PEs,
     * then one can say that it is using the first 2 PEs.
     * But if at the next simulation time the same Cloudlet can be
     * at the 3º position in this Collection, indicating that now it is using
     * the 3º and 4º Pe, which doesn't change anything. In real schedulers,
     * usually a process is pinned to a specific set of cores until it
     * finishes executing, to avoid the overhead of changing processes from
     * a run queue to another unnecessarily.</p>
     *
     * @return
     */
    @Override
    public List<CloudletExecution> getCloudletExecList() {
        //The method was overridden here just to extend its JavaDoc.
        return super.getCloudletExecList();
    }


    //NEED SIMILAR FUNCTION TO CHECK IF CLOUDLET NEEDS TO BE LEFT IN EXEC QUEUE

    /**
     * Checks which Cloudlets in the execution list have the virtual runtime
     * equals to their allocated time slice and preempt them, getting
     * the most priority Cloudlets in the waiting list (i.e., those ones
     * in the beginning of the list).
     *
     * @see #preemptExecCloudletsWithExpiredVRuntimeAndMoveToWaitingList()
     * @param currentTime {@inheritDoc}
     * @return {@inheritDoc}
     */
    @Override
    protected double moveNextCloudletsFromWaitingToExecList(final double currentTime) {
        System.out.println("[IN] : DRRHA > moveNextCloudletsFromWaitingToExecList");
        final List<CloudletExecution> finishedCloudlets = cloudletsFinished();

        final List<CloudletExecution> rerunCloudlet = cloudletsThatNeedToBeReRun();

        final List<CloudletExecution> preemptedCloudlets = preemptExecCloudletsWithExpiredVRuntimeAndMoveToWaitingList();

        int diff = getCloudletExecList().size()-(rerunCloudlet.size());
        incrementContextSwitch(diff);

        System.out.println("[IN] : DRRHA > moveNextCloudletsFromWaitingToExecList : context switches till now " + getContextSwitches());

        final double nextCloudletFinishTime = super.moveNextCloudletsFromWaitingToExecList(currentTime);


        /*After preempted Cloudlets are moved to the waiting list
        and next Cloudlets on the beginning of this list are moved
        to the execution list, the virtual runtime of these preempted Cloudlets
        is reset so that they can compete with other waiting Cloudlets to use
        the processor again.*/
        for(final CloudletExecution c: preemptedCloudlets) {
            c.setVirtualRuntime(computeCloudletInitialVirtualRuntime(c));
            c.setTimeSlice(computeCloudletTimeSlice(c));
        }



        for(CloudletExecution c: getCloudletWaitingList()){
            System.out.println("[IN] : DRRHA > moveNextCloudletsFromWaitingToExecList : Cloudlet in waiting list : "+c.getCloudletId());
        }

        return nextCloudletFinishTime;
    }

    /**
     *Checks which Cloudlets in the execution list have an expired virtual
     * runtime (that have reached the execution time slic) but fits the condition to be re run.
     * @return The list of preempted Cloudlets, that
     * must have their virtual runtime (VRT) reset after the next cloudlets are put into
     * the execution list
     */
    private List<CloudletExecution> cloudletsThatNeedToBeReRun(){
        System.out.println("[IN] : DRRHA > cloudletsThatNeedToBeReRun ");
        final Predicate<CloudletExecution> reRunCloudletLogic = cle -> cle.getVirtualRuntime() >= cle.getTimeSlice() && checkCondition(cle);
        final List<CloudletExecution> reRunCloudlets =
            getCloudletExecList()
                .stream()
                .filter(reRunCloudletLogic)
                .collect(Collectors.toList());

        for(CloudletExecution c: reRunCloudlets){
            c.setVirtualRuntime(computeCloudletInitialVirtualRuntime(c));
            c.setTimeSlice(computeCloudletTimeSlice(c));
            System.out.println("[IN] : DRRHA > cloudletsThatNeedToBeReRun : "+c.getCloudletId()+" will be run again");
        }

        return reRunCloudlets;
    }


    /**
     * Checks which Cloudlets in the execution list have an expired virtual
     * runtime (that have reached the execution time slice) and whose Computed Time Slice
     * is less than remaining burst time and
     * preempts its execution, moving them to the waiting list.
     *
     * @return The list of preempted Cloudlets, that were removed from the execution list
     * and must have their virtual runtime (VRT) reset after the next cloudlets are put into
     * the execution list.
     *
     */
    private List<CloudletExecution> preemptExecCloudletsWithExpiredVRuntimeAndMoveToWaitingList() {
        System.out.println("[IN] : DRRHA > preemptExecCloudletsWithExpiredVRuntimeAndMoveToWaitingList");
        final Predicate<CloudletExecution> vrtReachedTimeSlice = cle -> cle.getVirtualRuntime() >= cle.getTimeSlice();
        final List<CloudletExecution> expiredVrtCloudlets =
            getCloudletExecList()
                .stream()
                .filter(vrtReachedTimeSlice)
                .collect(toList());

         /*
        Cloudlets that are finished are being preempted
         */
        for(final CloudletExecution c: expiredVrtCloudlets){
            System.out.println("[IN]: DRRHA >  preemptExecCloudletsWithExpiredVRuntimeAndMoveToWaitingList : preempted cloudlets"+ c.getCloudletId());
        }

        expiredVrtCloudlets.forEach(cle -> addCloudletToWaitingList(removeCloudletFromExecList(cle)));
        return expiredVrtCloudlets;
    }


    /**
     *Checks which Cloudlets in the execution list have finished executing
     * @return The list of finished cloudlets that need to be removed from the exec list and put back into the finished list
     */
    private List<CloudletExecution> cloudletsFinished(){
        System.out.println("[IN]: DRRHA > cloudletsFinished ");
        final Predicate<CloudletExecution> noRemainingBurstTime = cle -> getRemainingBurstTime(cle) <= 0.0;
        final List<CloudletExecution> finishedCloudlets =
            getCloudletExecList()
                .stream()
                .filter(noRemainingBurstTime)
                .collect(Collectors.toList());

         /*
        Cloudlets that are finished are removed from exec list and moved to finished list
         */
        for(final CloudletExecution c: finishedCloudlets){
            System.out.println("[IN]: DRRHA > cloudletsFinished "+ c.getCloudletId());
//            c.getCloudlet().setStatus(Cloudlet.Status.SUCCESS);
            addCloudletToFinishedList(c);
        }
        return finishedCloudlets;
    }




    private int addCloudletsToFinishedList() {
        final List<CloudletExecution> finishedCloudlets
            = cloudletExecList.stream()
            .filter(cle -> cle.getCloudlet().isFinished())
            .collect(toList());

        for (final CloudletExecution c : finishedCloudlets) {
            addCloudletToFinishedList(c);
        }

        return finishedCloudlets.size();
    }

    private void addCloudletToFinishedList(final CloudletExecution cle) {
        setCloudletFinishTimeAndAddToFinishedList(cle);
        removeCloudletFromExecList(cle);
    }
    /**
     * Sets the finish time of a cloudlet and adds it to the
     * finished list.
     *
     * @param cle the cloudlet to set the finish time
     */
    private void setCloudletFinishTimeAndAddToFinishedList(final CloudletExecution cle) {
        final double clock = vm.getSimulation().clock();
        cloudletFinish(cle);
        cle.setFinishTime(clock);
    }





}
