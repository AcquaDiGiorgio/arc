#ifndef __GROUPING_GM_JOB_GROUP_H__
#define __GROUPING_GM_JOB_GROUP_H__

#include <ctime>
#include <map>
#include <list>
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

    bool isValid(const ARex::JobLocalDescription& jobRef) const;

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
    enum Status {
        New, Complete, 
    };

    fs::path controlDir;
    fs::path workDir;

    std::list<std::string> requiredInputNames;
    std::list<std::string> movedInputsList;
    std::list<std::string> outputs;
    std::list<std::string> RTEs;
    std::list<std::string> envs;

    std::string queue;

    bool areInputsMoved;
    bool execModified;

    // From GMJobRef
    std::string id;
    // ARex::JobLocalDescription jld;
    Arc::User user;
    std::list<std::string> exec;
    std::string stdout_;
    std::string stderr_;
    std::string pipeLocation;

    GroupedJob(ARex::GMJobRef& jobRef, const std::string jobControlDir, std::string pipeLocation):
        id(jobRef->get_id()),
        user(jobRef->get_user()),
        controlDir(fs::path(jobControlDir)), 
        workDir(jobRef->SessionDir()),
        areInputsMoved(false),
        execModified(false),
        pipeLocation(pipeLocation),
        queue(jobRef->GetLocalDescription()->queue),
        exec(jobRef->GetLocalDescription()->exec)
        {this->ProcessGrami();};

    bool AreInputsReady();
    void ProcessGrami();
    void ModifyExec();
    void TryCopyInputs(const std::string& newDir);
    void CreateEnv(const std::string& newDir);

    bool operator==(GroupedJob& other){ return id == other.id; }
};

class JobGroup : public std::list<GroupedJob> {
public:
    enum class GroupState{
        STORING, READY, SENT, FINISHED
    };

private:
    ARex::JobId id;

    ARex::JobId arcId;

    std::chrono::steady_clock::time_point firstJobTime{};
    
    std::string delegId;

    fs::path basePath;
    fs::path inputPath;

    GroupState state;

    int outputsMoved = 0;

    const unsigned int maxSize;
    const unsigned int maxSeconds;

public:
    void SendGroup(ARex::JobsList* jobs, const ARex::GMConfig& config);
    void SendExec();

    void AddToGroup(ARex::GMJobRef& jobRef, const std::string controlDir);

    JobGroup(std::string deleg_id, unsigned int max_size, unsigned int max_seconds);

    const ARex::JobId getId() const {return this->id;};
    const GroupState getState() const {return this->state;}

    long GetTimeElapsed();

    bool CheckInputsReady();
    void CheckReadiness();
    void SetReady();
    
    GroupedJob* GetGroupedJob(ARex::GMJobRef jobRef);
    void TryCopyInputsOfJob(ARex::GMJobRef jobRef);

private:
    ARex::GMJob buildGrouppedJob(void);
    void PrepareConfigFile(void);
};

class JobGroupDef {
public:
    const std::string name;   
    const unsigned int maxJobs;
    const unsigned int maxSeconds;

    const std::string senderDn;
    const std::string senderPEMLocation;

    const JLDChecker checker;

private:
    std::list<JobGroup> groups;

public:
    JobGroupDef(const std::string& name, const IFIC::GroupConfig& groupConfig, const ARex::GMConfig& arexConfig);

    JobGroup* AddToGroupDef(ARex::GMJobRef& jobRef, const ARex::GMConfig& config);
    
private:
    JobGroup* CreateNewGroup(const ARex::GMConfig& config);
};

class JobGroups {
    friend ARex::GMJob;
    friend JobGroup;
    friend JobGroupDef;

public:
    enum class GroupedStatus{
        NOT_PROCESSED, STORED, FINISHING
    };

private:
    static const int MAX_STORED_FINISHED_JOBS = 1024;

    static std::list<JobGroupDef> groupsInfo;
    static std::map<ARex::JobId, JobGroup*> jobToGroup; 

    static std::map<std::string, JobGroupDef*> groups;

    static void BuildGroups(const ARex::GMConfig& config);
    static void ReportDeletion(ARex::JobId jobId);
    static JobGroup* GetGroupOfJob(ARex::GMJobRef& jobRef);
    
public:
    static GroupedStatus TryAddJob(ARex::JobsList* jobList, ARex::GMJobRef& jobRef, const ARex::GMConfig& config);
};


} // namespace IFIC

#endif //__GROUPING_GM_JOB_GROUP_H__