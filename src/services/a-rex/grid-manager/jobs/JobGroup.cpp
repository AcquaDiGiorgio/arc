#include <cstddef>
#include <ctime>
#include <exception>
#include <fstream>
#include <list>
#include <mutex>
#include <ostream>
#include <string>
#include <sys/stat.h>
// #include <system_error>
#include <filesystem>

#include <arc/GUID.h>
#include "GMJob.h"

#include "../files/ControlFileContent.h"
#include "../../delegation/DelegationStore.h"
#include "../../delegation/DelegationStores.h"
#include "../files/ControlFileHandling.h"
#include "../conf/GMConfig.h"
#include "../../job.h"
#include "JobsList.h"

#include "JobGroup.h"

namespace fs = std::filesystem;

namespace IFIC {

std::map<std::string, std::unique_ptr<JobGroup>> JobGroups::groups = std::map<std::string, std::unique_ptr<JobGroup>>();
static Arc::Logger logger = Arc::Logger(Arc::Logger::getRootLogger(), "IFIC.JobGrouping", Arc::DEBUG);
std::mutex JobGroups::_lock;

//
// Utilities
//
template<typename T>
const std::string printList(std::list<T> list, std::string (* getter)(T)){
    std::string values = "[";
    for(typename std::list<T>::iterator it = list.begin(); it != list.end(); ++it){
        values += getter(*it);
        if (std::next(it) != list.end())
            values += ", ";
    }
    values += "]";
    return values;
}

const std::string getJLDValue(const ARex::JobLocalDescription& jld, std::string key){
    if(key == "queue") return jld.queue;
    if(key == "node") return jld.headnode;
    if(key == "host") return jld.headhost;
    if(key == "DN") return jld.DN;
    if(key == "client_name") return jld.clientname;
    if(key == "job_name") return jld.jobname;
    return "";
};

std::string GetDelegationId(const ARex::GMConfig& config, std::string senderPEMLocation, std::string senderDn){
    ARex::DelegationStores* delegs = config.GetDelegations();
    if (!delegs) {
        logger.msg(Arc::FATAL, "Failed to obtain delegations");
        return "";
    }

    ARex::DelegationStore& store = (*delegs)[config.DelegationDir()];

    std::ifstream pemFile(senderPEMLocation);
    std::stringstream buffer;

	buffer << pemFile.rdbuf();

    std::string credentials_pem = buffer.str();

    std::string deleg_id;  

    if (!store.AddCred(deleg_id, senderDn, credentials_pem)) {
        logger.msg(Arc::FATAL, "Failed to store credential for job group");
        return "";
    }

    return deleg_id;
}

std::string ParseLocalDescription(ARex::JobLocalDescription* localDesc){
    std::string parsed;

    parsed += "- Job Id: "      + localDesc->jobid + "\n";
    parsed += "- Job Name: "    + localDesc->jobname + "\n";
    parsed += "- Global Id: "   + localDesc->globalid + "\n";
    parsed += "- Head Node: "   + localDesc->headnode + "\n";
    parsed += "- Head Host: "   + localDesc->headhost + "\n";
    parsed += "- Global URL: "  + localDesc->globalurl + "\n";
    parsed += "- Interface: "   + localDesc->interface + "\n";
    parsed += "- LRMS: "        + localDesc->lrms + "\n";
    parsed += "- Queue: "       + localDesc->queue + "\n";
    parsed += "- Local Id: "    + localDesc->localid + "\n";
    
    parsed += "- Exec:";
    for (auto str: localDesc->exec)
        parsed += " " + str;
    parsed += "\n";
    
    parsed += "- DN: "              + localDesc->DN + "\n";
    parsed += "- Start Time: "      + localDesc->starttime.str(Arc::TimeFormat::UserTime) + "\n";
    parsed += "- Life Time: "       + localDesc->lifetime + "\n";
    parsed += "- Process Time: "    + localDesc->processtime.str(Arc::TimeFormat::UserTime) + "\n";
    parsed += "- Exec Time: "       + localDesc->exectime.str(Arc::TimeFormat::UserTime) + "\n";
    parsed += "- Notify: "          + localDesc->notify + "\n";
    parsed += "- Client Name: "     + localDesc->clientname + "\n";
    parsed += "- Software: "        + localDesc->clientsoftware + "\n";
    parsed += "- Delegation Id: "   + localDesc->delegationid + "\n";
    
    int reruns = -1;
    if (localDesc->reruns)
        reruns = localDesc->reruns;
    parsed += "- Reruns: "          + std::to_string(reruns) + "\n";

    int priority = -1;
    if (localDesc->priority)
        priority = localDesc->priority;
    parsed += "- Priority: "        + std::to_string(priority) + "\n";

    int downloads = -1;
    if (localDesc->downloads)
        downloads = localDesc->downloads;
    parsed += "- Donwloads: "       + std::to_string(downloads) + "\n";
    
    int uploads = -1;
    if (localDesc->uploads)
        uploads = localDesc->uploads;
    parsed += "- Uploads: "         + std::to_string(uploads) + "\n";
    
    parsed += "- Session Dir: "     + localDesc->sessiondir + "\n";
    
    parsed += "- Project Names: ";
    for (auto str: localDesc->projectnames)
        parsed += " " + str;
    parsed += "\n";
    
    parsed += "- Job Report: ";
    for (auto str: localDesc->jobreport)
        parsed += " " + str;
    parsed += "\n";
    
    parsed += "- Local VO:";
    for (auto str: localDesc->localvo)
        parsed += " " + str;
    parsed += "\n";
    
    parsed += "- Voms:";
    for (auto str: localDesc->voms)
        parsed += " " + str;
    parsed += "\n";

    parsed += "- Auth Groups:";
    for (auto str: localDesc->authgroups)
        parsed += " " + str;
    parsed += "\n";
    
    parsed += "- Input Data:\n";
    for (auto file: localDesc->inputdata){
        parsed += "\n > PFN: " + file.pfn + "\n";
        parsed += "\n > LFN: " + file.lfn + "\n";
        parsed += "\n > Cred: " + file.cred + "\n";
        parsed += "\n > Cred Type: " + file.cred_type + "\n";
    }
    
    parsed += "- Output Data:\n";
    for (auto file: localDesc->outputdata){
        parsed += "\n > PFN: " + file.pfn + "\n";
        parsed += "\n > LFN: " + file.lfn + "\n";
        parsed += "\n > Cred: " + file.cred + "\n";
        parsed += "\n > Cred Type: " + file.cred_type + "\n";
    }
    
    parsed += "- RTEs:";
    for (auto str: localDesc->rte)
        parsed += " " + str;
    parsed += "\n";
    
    parsed += "- Action: "  + localDesc->action + "\n";
    parsed += "- STDin: "   + localDesc->stdin_ + "\n";
    parsed += "- STDout: "  + localDesc->stdout_ + "\n";
    parsed += "- STDerr: "  + localDesc->stderr_ + "\n";

    return parsed + "\n";
}

void IFIC::JobGroup::PrepareConfigFile(){
    // Move/Copy all files of each job to a tmp dir renaming them with their job id
    // find . -type f -exec cp '{}' './test/{}_$groupid' \;

    // Set exec to job exec

    std::string group_config;

    group_config = "export JOB_IDS = ( ";

    for(auto job : *this)
        group_config += job->get_id() + " ";
    
    group_config += ")\n";

    group_config += "export BUNDLE_ID = " + id + "\n";

    std::ofstream file(fs::path(basePath).append("groupConfig"));

    file << group_config;
}


//
// JLDChecker
//
bool JLDChecker::isValid(const ARex::JobLocalDescription& jobRef){
    logger.msg(
        Arc::DEBUG,
         "Checking job '%s' following the condition: '%s' '%s' '%s'",
          jobRef.jobid, by, condition, check_value);

    const std::string var = IFIC::getJLDValue(jobRef, by);

    if (var == ""){
        logger.msg(Arc::WARNING, "Var '%s' not found in JLD", by);
        return false;
    }

    if (condition == "eq")
        return var == check_value;

    if (condition == "neq")
        return var != check_value;

    if (condition == "in")
        return check_value.find(var) != std::string::npos;

    return false;
};

//
// JobGroup
//
JobGroup::JobGroup(std::string name, const IFIC::GroupConfig config):
        name(name),
        checker(JLDChecker(config.by, config.condition, config.check_value)),
        max_jobs(config.max_jobs),
        max_seconds(config.max_seconds),
        senderDn(config.senderDn),
        senderPEMLocation(config.senderPEMLocation)
{
    std::string uuid = Arc::UUID();
    size_t pos = uuid.find_last_of("-");
    id = uuid.substr(pos+1);

    basePath = fs::path("/tmp")
                .append("groups")
                .append(id);

    inputPath = fs::path(basePath)
                    .append("inputs");

    mkdir(basePath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    mkdir(inputPath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
};


void JobGroup::sendGroup(ARex::JobsList* jobList, const ARex::GMConfig& config){
    std::string uuid = Arc::UUID();
    size_t pos = Arc::UUID().find_last_of("-");
    ARex::JobId id = uuid.substr(pos+1);
    logger.msg(Arc::DEBUG, "Group job id: %s", id);

    Arc::User owner = Arc::User(0); // TODO: SET OWNER

    std::string sessionDir = config.SessionRoot("") + '/' + id;

    ARex::GMJobRef job(new ARex::GMJob(id, owner, sessionDir, ARex::JOB_STATE_ACCEPTED));
    
    logger.msg(Arc::DEBUG, "Setting up local desc");

    ARex::JobLocalDescription* local = new ARex::JobLocalDescription();

    local->jobid = id;
    local->DN = senderDn;
    local->lrms = config.DefaultLRMS();  // TODO: NEEDED??
    local->queue = this->front()->GetLocalDescription()->queue;
    local->exec.push_back("/bin/true"); // TODO: THIS HAS TO BE THE EXEC THAT LAUNCHES THE MERGED JOBS
    local->sessiondir = sessionDir;
    
    logger.msg(Arc::DEBUG, "Setting inputs and outputs");
    local->inputdata = std::list<ARex::FileData>();
    local->outputdata = std::list<ARex::FileData>();

    // !!!! CRITICAL !!!!
    // TODO: CHECK REST OF LocalDescription PARAMETERS (SUCH AS RTEs)

    logger.msg(Arc::DEBUG, "Merging jlds");
    for (ARex::GMJobRef job : *this){
        ARex::JobLocalDescription* jobLocal = job->GetLocalDescription();

        if (! jobLocal->inputdata.empty())
            local->inputdata.insert(
                local->inputdata.end(), 
                jobLocal->inputdata.begin(), 
                jobLocal->inputdata.end());

        if (! jobLocal->outputdata.empty())
            local->outputdata.insert(
                local->outputdata.end(), 
                jobLocal->outputdata.begin(), 
                jobLocal->outputdata.end());
    }

    logger.msg(Arc::DEBUG, "Obtaining delegation");
    ARex::DelegationStores* delegs = config.GetDelegations();
    if (!delegs) {
        logger.msg(Arc::FATAL, "Failed to obtain delegations");
        return;
    }

    std::string deleg_id = GetDelegationId(config, senderPEMLocation, senderDn);

    if (deleg_id == "") {
        return;
    }

    local->delegationid = deleg_id;
    

    if (! config.JobGroups(name)->dry_run) {
        // std::error_code e;

        // std::filesystem::create_directory(sessionDir, e);
        // if (e.value() != 0)
        //     logger.msg(Arc::ERROR, "Failed to create job directory, error: %s", e.value());
        

        // std::filesystem::create_directory(config.ControlDir() + id, e);
        // if (e.value() != 0)
        //     logger.msg(Arc::ERROR, "Failed to create job directory, error: %s", e.value());
        
        // ARex::job_local_write_file(*job, config, *local);

        // fs::path baseDir(local->sessiondir);
        // fs::path emptyFile("GROUPED");
        // std::ofstream file(baseDir / emptyFile);

        // logger.msg(Arc::INFO, "Requesting attention of group '%s'", name);
        // jobList->RequestAttention(job);
    }
    else {
        logger.msg(
            Arc::INFO, "DryRun activated for group %s", 
            name
        );
        logger.msg(
            Arc::INFO, 
            "Would have sent job (%s) with delegationId (%s) to queue (%s)",
            id, local->delegationid, local->queue 
        );
        logger.msg(
            Arc::INFO, 
            "Job (%s) has '%s' as executable, '%s' as inputs and '%s' as outputs",
            id,
            printList<std::string>(
                local->exec, 
                [](std::string c){ return c; }),
            printList<ARex::FileData>(
                local->inputdata, 
                [](ARex::FileData c){ return c.lfn; }),
            printList<ARex::FileData>(
                local->outputdata, 
                [](ARex::FileData c){ return c.lfn; })
        );
    }

    firstJobTime = std::chrono::steady_clock::time_point{};
};

JobGroup::GroupState JobGroup::addToGroup(ARex::GMJobRef &jobRef){
    std::string jobId = jobRef->get_id();

    // TODO: Check grami file at 
    //  ARex::job_control_path(config.ControlDir(), jobRef->get_id(), NULL);
    //  for input change
    for (ARex::GMJobRef _job : *this){
        if (_job->get_id() == jobRef->get_id()){ // Already in group
            logger.msg(Arc::WARNING, "Job '%s' already in group '%s'", jobId, name);
            return JobGroup::GroupState::STORED;
        }
    }

    if (! checker.isValid(*jobRef->GetLocalDescription())){
        logger.msg(Arc::DEBUG, "Group '%s' failed JLD check", name);
        return JobGroup::GroupState::FAILED;
    }

    logger.msg(Arc::INFO, "Adding job '%s' to group '%s'", jobRef->get_id(), name);
    
    this->push_back(jobRef);

    std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();

    std::string job_dir = jobRef->GetLocalDescription()->sessiondir;
    logger.msg(Arc::VERBOSE, "Copying input files at %s", job_dir);

    for (auto entry : fs::directory_iterator(job_dir)){
        logger.msg(Arc::DEBUG, "> Copying '%s'", entry.path().filename().generic_string());
        const std::string newName = entry.path().filename().generic_string() + "_" + jobRef->get_id();
        
        fs::copy_file(
            entry.path(), 
            fs::path(inputPath).append(newName));
    };

    // TODO: Create a callback timer 
    if (firstJobTime == std::chrono::steady_clock::time_point{}){
        firstJobTime = now;
        return JobGroup::GroupState::STORED;
    }

    logger.msg(Arc::DEBUG, "Checking callback");
    long elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - firstJobTime).count();

    if (this->size() >= max_jobs || elapsed > max_seconds)
        return JobGroup::GroupState::READY;

    return JobGroup::GroupState::STORED;
};

//
// JobGroups
//
void JobGroups::buildGroups(const ARex::GMConfig& config){
    if(!groups.empty())
        return;

    int res = mkdir("/tmp/groups", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    int err = errno;
    
    if (res != 0 && err != EEXIST){
        logger.msg(Arc::FATAL, "Error while creating directory '/tmp/groups': %s", err);
        throw std::exception();
    }

    for(std::string name : config.JobGroupNames()){
        logger.msg(Arc::DEBUG, "No groups found with name '%s', creating one", name);
        groups.insert(std::pair(
            name, 
            new JobGroup(
                name, 
                *config.JobGroups(name)
            )
        ));
    }
}

bool JobGroups::tryAddJob(ARex::JobsList* jobList, ARex::GMJobRef& jobRef, const ARex::GMConfig& config){
    std::string dirStrPath = jobRef->SessionDir();

    logger.msg(Arc::VERBOSE, "Looking for GROUPED file in %s", dirStrPath);

    const std::filesystem::path dirPath(dirStrPath);
    fs::directory_iterator dirIter = fs::directory_iterator(fs::directory_entry(dirPath));
    
    for (fs::directory_entry entry : dirIter) {
        if (entry.is_regular_file() && entry.path().filename() == "GROUPED"){
            logger.msg(Arc::VERBOSE, "The job is a group, won't try to add it");
            return false;
        }
    }

    buildGroups(config);

    for (std::map<std::string, std::unique_ptr<JobGroup>>::iterator iter = groups.begin(); iter != groups.end(); ++iter){
        std::string jobCtrl = ARex::job_control_path(config.ControlDir(), jobRef->get_id(), NULL);

        for (auto entry : fs::directory_iterator(jobCtrl)){
            if (entry.path().filename() == "input_status")
                logger.msg(Arc::DEBUG, "Printing content of input_status");

            else if (entry.path().filename() == "xml")
                logger.msg(Arc::DEBUG, "Printing content of xml");

            else if (entry.path().filename() == "grami")
                logger.msg(Arc::DEBUG, "Printing content of grami");

            else
                continue;

            std::ifstream file(entry.path().generic_string());
            std::stringstream buffer;
            buffer << file.rdbuf();
            logger.msg(Arc::DEBUG, buffer.str());
        }

        auto status = iter->second->addToGroup(jobRef);

        if (status == JobGroup::GroupState::FAILED){
            logger.msg(Arc::DEBUG, "Job group not stored at %s", iter->first);
            continue; // Try on another group
        }
        
        if (status == JobGroup::GroupState::READY){
            logger.msg(Arc::INFO, "Job group '%s' ready to be sent", iter->first);
            // iter->second->sendGroup(jobList, config);
            iter->second->SendExec(config);
        }
        else if (status == JobGroup::GroupState::STORED){
            logger.msg(Arc::DEBUG, "The job stored in a group given by '%s'", iter->first);
        }

        return true;    
    }

    logger.msg(Arc::VERBOSE, "No valid groups to store the job at");
    return false;
};

std::string buildFileDesc(ARex::FileData file){
    std::string location;

    if (file.has_lfn())
        location = file.lfn;
    else
        location = file.pfn;
    
    std::string fileBase = basename(file.lfn.c_str());

    return "(\"" + fileBase + "\" \"" + location + "\")\n";
}

void JobGroup::SendExec(const ARex::GMConfig& config){
    // General Info
    PrepareConfigFile();

    logger.msg(Arc::VERBOSE, "Building XLRS");
    
    logger.msg(Arc::DEBUG, "Job Descriptions of Group");
    
    for (auto job : *this)
        logger.msg(Arc::DEBUG, ParseLocalDescription(job->GetLocalDescription()));
    
    logger.msg(Arc::DEBUG, "> Building general info");
    
    std::string desc_str =
        "&(executable=/arc/launch_group.sh)\n"
        "(jobname=grouped_" + getId() + ")\n"
        "(stdout=std.out)\n"
        "(stderr=std.err)\n";
    
    // Inputs
    logger.msg(Arc::DEBUG, "> Building input files");
    desc_str += "(inputfiles=\n";
    for (ARex::GMJobRef ref : *this)
        for(ARex::FileData file : ref->GetLocalDescription()->inputdata)
            desc_str += buildFileDesc(file);
    
    for (auto entry : fs::directory_iterator(inputPath)){
        if (entry.is_regular_file()){
            fs::path input = entry.path();

            if (! input.is_absolute())
                input = fs::absolute(input);
            
            desc_str += "(\"" + input.generic_string() + "\" \"" + input.filename().generic_string() + "\")\n";
        }
    }

    desc_str += "(\"groupConfig\" \"" + fs::path(basePath).append("groupConfig").generic_string() + "\")\n";
    desc_str += "(\"GROUPED\" \"/arc/GROUPED\"))\n";
        
    // Outputs
    logger.msg(Arc::DEBUG, "> Building output files");
    desc_str += "(outputfiles=\n";

    for (ARex::GMJobRef ref : *this) 
        for(ARex::FileData file : ref->GetLocalDescription()->outputdata)
            desc_str += buildFileDesc(file);
    
    desc_str += "(\"/\" \"\" ))\n";
    
    // RTEs
    logger.msg(Arc::DEBUG, "> Building RTEs");
    if(false){
        desc_str += "(runtimeEnvironment=\n";
        for (ARex::GMJobRef ref : *this) {
            desc_str+="\"output.txt\"\n";
        }
        desc_str += ")\n";
    }

    // Environ
    logger.msg(Arc::DEBUG, "> Building environment");
    if (false){
        desc_str += "(environment=\n";
        for (ARex::GMJobRef ref : *this) {
            desc_str+="(\"ENV_VAR\" \"VAR\" )\n";
        }
        desc_str += ")";
    }

    std::string deleg_id = GetDelegationId(config, senderPEMLocation, senderDn);  
    if (deleg_id == "") {
        return;
    }

    // Rest
    logger.msg(Arc::DEBUG, "> Building final info");
    desc_str += 
        // "(executables=workloadExec.sh)\n" // ?????
        "(queue=" + front()->GetLocalDescription()->queue + ")\n"
        "(delegationid=" + deleg_id + ")\n";


    std::string xlrsPath("/tmp/group_" + getId() + ".xlrs");

    logger.msg(Arc::DEBUG, "Saving group XLRS at %s", xlrsPath);

    std::ofstream stream(xlrsPath);
    stream << desc_str;
    stream.close();

    __pid_t pid = fork();

    logger.msg(Arc::DEBUG, "Launching execlp");
    if (pid < 0)
        throw std::exception();

    if (pid == 0){
        execlp(
            "arcsub", 
            "arcsub",
            "-d", 
            "DEBUG", 
            "-C", 
            "$HOSTNAME", 
            "-T", 
            "arcrest", 
            "-Q", 
            "None", 
            xlrsPath.c_str(), 
            NULL
        );
        logger.msg(Arc::DEBUG, "CHILD DONE");
        return;
    }
    
    logger.msg(Arc::DEBUG, "PARENT DONE");
};

}

//
// GMConfig
//
const IFIC::GroupConfig* ARex::GMConfig::JobGroups(std::string groupName) const {
  std::map<std::string, IFIC::GroupConfig>::const_iterator iter = job_groups.find(groupName);
  
  if (iter == job_groups.end()){
    logger.msg(Arc::WARNING, "Unable to find group '%s'", groupName);
    return nullptr;
  }

  return &(iter->second);
}

const std::list<std::string> ARex::GMConfig::JobGroupNames() const {
    std::list<std::string> names = std::list<std::string>();

    for (std::pair<const std::string, IFIC::GroupConfig> pair : job_groups){
        names.push_front(pair.first);
    }

    return names;
}

