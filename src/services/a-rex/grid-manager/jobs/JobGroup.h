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
    JLDChecker(std::string _by, std::string _condition, std::string _check_value): 
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
    bool requirementsProcessed;
    
    ARex::GMJobRef& jobRef;

    GroupedJob(ARex::GMJobRef& jobRef, std::string jobControlDir) : 
        jobRef(jobRef), 
        controlDir(fs::path(jobControlDir)), 
        workDir(jobRef->SessionDir()),
        inputsMoved(false),
        requirementsProcessed(false)
        {};

    bool AreInputsReady(const ARex::GMConfig& config);
    void MoveInputs(const std::string& newLocation, bool copyInstread=true);

    bool operator==(GroupedJob other){ return jobRef->get_id() == other.jobRef->get_id(); }
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
    void SendExec(const ARex::GMConfig& config);

    GroupState addToGroup(ARex::GMJobRef& jobRef, const ARex::GMConfig& config);

    JobGroup(std::string name, const IFIC::GroupConfig config);

    ARex::JobId getId(){return id;};
};

class JobGroups {
    friend ARex::GMJob;
    friend JobGroup;

private:
    static std::map<std::string, std::unique_ptr<JobGroup>> groups;
    static void addToGroup(const std::string& groupName, ARex::GMJobRef& jobRef, const ARex::GMConfig& config);
    static void buildGroups(const ARex::GMConfig& config);
    static std::mutex _lock;
    
public:
    static bool tryAddJob(ARex::JobsList* jobList, ARex::GMJobRef& jobRef, const ARex::GMConfig& config);
};


} // namespace IFIC

#endif