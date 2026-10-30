#ifndef __GROUPING_GM_JOB_GROUP_H__
#define __GROUPING_GM_JOB_GROUP_H__

#include <ctime>
#include <map>
#include <list>
#include <mutex>
#include <strings.h>
#include <filesystem>

#include <arc/Logger.h>

#include "../files/ControlFileHandling.h"

#include "GMJob.h"

namespace fs = std::filesystem;

namespace IFIC {

class GroupConfig {
    friend ARex::GMConfig;

public:
    int max_jobs;
    long max_seconds;

    std::string by;
    std::string condition;
    std::string check_value;

    std::string senderDn;
    std::string senderPEMLocation;

    bool dry_run = false;
};

class JLDChecker {
private:
    std::string by;
    std::string condition;
    std::string check_value;

public:
    JLDChecker(const std::string _by, const std::string _condition, const std::string _check_value): 
        by(_by), condition(_condition), check_value(_check_value) {};

    bool isValid(const ARex::JobLocalDescription& jobRef);

    bool operator=(JLDChecker& jld){
        return (
            by == jld.by and
            condition == jld.condition and
            check_value == jld.check_value
        );
    };

    bool operator=(JLDChecker jld){
        return (
            by == jld.by and
            condition == jld.condition and
            check_value == jld.check_value
        );
    };
};

class GroupedJob {
public:
    fs::path controlDir;
    fs::path workDir;

    std::list<std::string> requiredInputNames;
    std::list<std::string> outputs;
    std::list<std::string> RTEs;
    std::list<std::string> envs;

    bool inputsMoved;

    // From GMJobRef
    std::string id;
    ARex::JobLocalDescription jld;
    Arc::User user;

    std::string stdout_;
    std::string stderr_;

    GroupedJob(ARex::GMJobRef& jobRef, const std::string jobControlDir) :
        id(jobRef->get_id()),
        jld(*jobRef->GetLocalDescription()),
        user(jobRef->get_user()),
        controlDir(fs::path(jobControlDir)), 
        workDir(jobRef->SessionDir()),
        inputsMoved(false)
        {this->ProcessGrami();};

    bool AreInputsReady();
    void ProcessGrami();
    void MoveInputs(const std::string& newLocation, bool copyInstread=true);

    bool operator==(GroupedJob other){ return id == other.id; }
};

class JobGroup : public std::list<GroupedJob> {
public:
    enum class GroupState{
        STORED, FAILED, READY
    };

private:
    ARex::JobId id;

    std::string name;
    int max_jobs;
    long max_seconds;
    std::chrono::steady_clock::time_point firstJobTime{};

    std::string senderDn;
    std::string senderPEMLocation;

    JLDChecker checker;

    fs::path basePath;
    fs::path inputPath;

    ARex::GMJob buildGrouppedJob(void);
    void PrepareConfigFile(void);

public:
    void SendGroup(ARex::JobsList* jobs, const ARex::GMConfig& config);
    std::string SendExec(const ARex::GMConfig& config);

    GroupState addToGroup(ARex::GMJobRef& jobRef, const std::string controlDir);

    JobGroup(const std::string name, const IFIC::GroupConfig config);

    ARex::JobId getId(){return id;};

    void Reset();
};

class JobGroups {
    friend ARex::GMJob;
    friend JobGroup;

public:
    enum class GroupedStatus{
        NOT_PROCESSED, STORED, FINISHED, FINISHING
    };

private:
    static const int MAX_STORED_FINISHED_JOBS = 1024;

    static std::map<std::string, std::unique_ptr<JobGroup>> groups;
    static std::map<std::string, std::string> sentJobsToGroupId;
    static std::list<std::string> finishedJobs;

    static std::mutex sentJobs_lock;
    static std::mutex finishedJobs_lock;

    static void BuildGroups(const ARex::GMConfig& config);

    static bool IsJobSent(const ARex::GMJobRef& jobRef);
    static bool HasOutput(const ARex::GMJobRef& jobRef);
    static bool HasJobFinished(const ARex::GMJobRef& jobRef);

    static void ReturnOutputs(const ARex::GMJobRef& jobRef);
    static void SaveFinishedJob(const ARex::GMJobRef& jobRef);

public:
    static GroupedStatus TryAddJob(ARex::JobsList* jobList, ARex::GMJobRef& jobRef, const ARex::GMConfig& config);
};


} // namespace IFIC

#endif //__GROUPING_GM_JOB_GROUP_H__