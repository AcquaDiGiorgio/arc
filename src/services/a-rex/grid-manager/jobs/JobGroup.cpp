#include <cstddef>
#include <ctime>
#include <exception>
#include <fstream>
#include <list>
#include <memory>
#include <regex>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <filesystem>
#include <sys/wait.h> 
#include <unistd.h> 

#include <arc/GUID.h>

#include "GMJob.h"
#include "JobsList.h"

#include "../conf/GMConfig.h"
#include "../files/ControlFileContent.h"
#include "../files/ControlFileHandling.h"
#include "../../job.h"
#include "../../delegation/DelegationStore.h"
#include "../../delegation/DelegationStores.h"

#include "JobGroup.h"

namespace fs = std::filesystem;

namespace IFIC {

std::map<std::string, JobGroupDef*> JobGroups::groups = std::map<std::string, JobGroupDef*>();
std::map<ARex::JobId, JobGroup*> JobGroups::jobToGroup = std::map<ARex::JobId, JobGroup*>(); 

static Arc::Logger logger = Arc::Logger(Arc::Logger::getRootLogger(), "IFIC.JobGrouping", Arc::DEBUG);

//
// Utilities
//
template<typename T>
const std::string printList(std::list<T>& list, const std::string& (* getter)(const T&)){
    std::string values = "[";

    for(typename std::list<T>::const_iterator it = list.begin(); it != list.end(); ++it)
        values += " " + getter(*it) + ",";

    if(values.length() > 1)
        values.pop_back();

    values += " ]";
    return values;
};

const std::string getJLDValue(const ARex::JobLocalDescription& jld, std::string key){
    if(key == "queue") return jld.queue;
    if(key == "node") return jld.headnode;
    if(key == "host") return jld.headhost;
    if(key == "DN") return jld.DN;
    if(key == "client_name") return jld.clientname;
    if(key == "job_name") return jld.jobname;
    return "";
};

std::string GetDelegationId(const ARex::GMConfig& config, const std::string senderPEMLocation, std::string senderDn){
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
};

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
};

std::list<std::string> CheckRegex(const std::string content, const std::string regexStr){
    std::list<std::string> retVal;

    std::regex regex(regexStr);

    std::sregex_iterator regex_begin = 
        std::sregex_iterator(content.begin(), content.end(), regex);
    std::sregex_iterator regex_end = std::sregex_iterator();

    for (std::sregex_iterator i = regex_begin; i != regex_end; ++i){
        std::smatch match = *i;
        std::string match_str = match.str();
        
        retVal.push_back(match_str);
    }

    if (!retVal.empty())
        logger.msg(
            Arc::DEBUG, "Regex pattern '%s' Found: %s", 
            regexStr, 
            printList<std::string>(retVal, [](const std::string& c) -> const std::string& { return c; })
        );
    else
        logger.msg(Arc::DEBUG, "Regex pattern '%s' found nothing", regexStr);

    return retVal;
};

std::string BuildFileDesc(ARex::FileData file){
    std::string location;

    if (file.has_lfn())
        location = file.lfn;
    else
        location = file.pfn;

    std::string fileBase = basename(file.lfn.c_str());

    return "(\"" + fileBase + "\" \"" + location + "\")\n";
};

std::list<std::string> CutRegex(const std::list<std::string> matches, const char splitChar, const int trimBack=0, const int trimFront=0){
    std::list<std::string> newMatches; 
    for(std::string entry : matches){
        size_t pos = entry.find(splitChar);
        std::string modifiedEntry = entry.substr(pos+1+trimBack, entry.size()-pos-1-trimBack-trimFront);
        newMatches.push_back(modifiedEntry);
    }
    return newMatches;
};

bool IsGroupedInGrami(const std::string controlDir){
    fs::path grami = fs::path(controlDir).append("grami");

    std::ifstream gramiStream(grami);

    std::stringstream gramiBuffer;
    gramiBuffer << gramiStream.rdbuf();
    std::string gramiContent = gramiBuffer.str();

    gramiStream.close();

    std::list<std::string> requiredInputNames_  = CutRegex(
        CheckRegex(gramiContent, "joboption_inputfile_[0-9]+='.*'"),
        '=', 1, 1);

    for(std::string input : requiredInputNames_)
        if (input == "/GROUPED")
            return true;

    return false;
};

std::pair<std::string, std::string> ParseGramiOption(std::string gramiLine){
    const std::string prefix = "joboption_";

    size_t separator = gramiLine.find_first_of('=');

    std::string key = gramiLine.substr(prefix.size()+1, separator);
    std::string value = gramiLine.substr(separator+1);

    if (value.empty())
        return std::pair<std::string, std::string>();

    if (value[0] == '\'')
        value = value.substr(1, value.size()-1);

    size_t undercorePos = key.find('_');
    if(undercorePos != std::string::npos)
        key = key.substr(0, undercorePos-1);

    return std::pair<std::string, std::string>(key, value);
};

std::map<std::string, std::list<std::string>> ParseGrami(const ARex::GMConfig config, ARex::GMJobRef& jobRef){
    std::map<std::string, std::list<std::string>> optionsMap;
    
    std::string controlDir = ARex::job_control_path(config.ControlDir(), jobRef->get_id(), NULL);
    fs::path grami = fs::path(controlDir).append("grami");

    std::ifstream gramiStream(grami);

    std::string line;
    while (std::getline(gramiStream, line)){
        // Parse grami otions such as:
        //  joboption_directory='/var/spool/arc/sessiondir/XXXXXXXXX'
        //  joboption_inputfile_0='/localfile.txt'
        std::pair<std::string, std::string> optionPair = ParseGramiOption(line);

        if(optionsMap.find(optionPair.first) == optionsMap.end()){
            optionsMap.insert(
            std::pair<std::string, std::list<std::string>>(
                optionPair.first, std::list<std::string>()
            ));
        }

        optionsMap[optionPair.first].push_back(optionPair.second);
    }

    return optionsMap;
};

//
// JLDChecker
//
bool JLDChecker::isValid(const ARex::JobLocalDescription& jobRef) const{
    logger.msg(
        Arc::DEBUG,
         "Checking job '%s' following the condition: '%s' '%s' '%s'",
          jobRef.globalid, this->by, this->condition, this->check_value);

    const std::string var = IFIC::getJLDValue(jobRef, by);

    if (var == ""){
        logger.msg(Arc::WARNING, "Var '%s' not found in JLD", this->by);
        return false;
    }

    if (this->condition == "eq")
        return var == this->check_value;

    if (this->condition == "neq")
        return var != this->check_value;

    if (this->condition == "in")
        return this->check_value.find(var) != std::string::npos;

    return false;
};

//
// GroupedJob
//

bool GroupedJob::AreInputsReady(){
    logger.msg(Arc::DEBUG, "Checking input files at workdir %s", this->workDir.generic_string());

    std::list<std::string> files;

    for (auto entry : fs::directory_iterator(this->workDir)){
        if (entry.is_regular_file()){
            std::string input = entry.path().filename().generic_string();
            logger.msg(Arc::DEBUG, "Found %s", input);
            files.push_front(input);
        }
    }

    size_t filesSize = files.size();
    size_t reqInputsSize = this->requiredInputNames.size();

    if (reqInputsSize != filesSize){
        logger.msg(Arc::DEBUG, "Not all inputs downloaded: %d/%d", (int)filesSize, (int)reqInputsSize);
        return false;
    }

    for (std::string reqInput : this->requiredInputNames){
        bool found = false;

        for(std::string foundInput : files){
            if(foundInput == reqInput){
                found = true;
                break;
            }
        }

        if(!found){
            logger.msg(Arc::FATAL, "Grouped inputs mismatch for file: %s", reqInput);
            logger.msg(Arc::FATAL, "Required files: %s", 
                printList<std::string>(
                    requiredInputNames,
                    [](const std::string& c) -> const std::string& { return c; }));
            logger.msg(Arc::FATAL, "Found files: %s",
                printList<std::string>(
                    files,
                    [](const std::string& c) -> const std::string& { return c; }));
            return false;
        }
        else
            files.remove(reqInput);
    }

    logger.msg(Arc::DEBUG, "All inputs are ready!");
    return true;
};

void GroupedJob::ProcessGrami(){
    logger.msg(Arc::DEBUG, "Processing grami file at %s", this->controlDir.generic_string());

    fs::path grami = fs::path(this->controlDir).append("grami");

    std::ifstream gramiStream(grami);

    std::stringstream gramiBuffer;
    gramiBuffer << gramiStream.rdbuf();
    std::string gramiContent = gramiBuffer.str();

    gramiStream.close();

    std::list<std::string> requiredInputNames_  = CutRegex(
        CheckRegex(gramiContent, "joboption_inputfile_[0-9]+='.*'"),
        '=', 1, 1);

    for(std::string input : requiredInputNames_)
        this->requiredInputNames.push_back(fs::path(input).filename().generic_string());

    this->outputs = CutRegex(
        CheckRegex(gramiContent, "joboption_outputfile_[0-9]+='.*'"), 
        '=', 1, 1);
    this->envs    = CutRegex(
        CheckRegex(gramiContent, "joboption_env_[0-9]+='.*'"), 
        '=', 1, 1);
    this->RTEs    = CutRegex(
        CheckRegex(gramiContent, "joboption_runtime_[0-9]+(_[0-9]+)?='.*'"), 
        '=', 1, 1);
    this->stdout_ = CutRegex(
        CheckRegex(gramiContent, "joboption_stdout='.*'"), 
        '=', 1, 1)
        .front();
    this->stderr_ = CutRegex(
        CheckRegex(gramiContent, "joboption_stderr='.*'"), 
        '=', 1, 1)
        .front();
    
    size_t last_slash_out = this->stdout_.find_last_of('/');
    size_t last_slash_err = this->stderr_.find_last_of('/');
    
    this->stdout_ = this->stdout_.substr(last_slash_out+1);
    this->stderr_ = this->stderr_.substr(last_slash_err+1);

    logger.msg(Arc::DEBUG, "Final info:");
    logger.msg(Arc::DEBUG, "Required Input Names: %s",
        printList<std::string>(
            this->requiredInputNames,
            [] (const std::string& c) -> const std::string& { return c; }));
    logger.msg(Arc::DEBUG, "Outputs: %s",
        printList<std::string>(
            this->outputs,
            [] (const std::string& c) -> const std::string& { return c; }));
    logger.msg(Arc::DEBUG, "Envs: %s",
        printList<std::string>(
            this->envs,
            [] (const std::string& c) -> const std::string& { return c; }));
    logger.msg(Arc::DEBUG, "RTEs: %s",
        printList<std::string>(
            this->RTEs,
            [] (const std::string& c) -> const std::string& { return c; }));
    logger.msg(Arc::DEBUG, "Stdout: %s", this->stdout_);
    logger.msg(Arc::DEBUG, "Stderr: %s", this->stderr_);
};

void GroupedJob::MoveInputs(const std::string& newDir, bool copyInstead){
    if(this->inputsMoved)
        return;

    bool inputsFailed = false;
    std::string suffix = "_" + this->id;

    logger.msg(Arc::DEBUG, "Moving inputs of job %s", this->id);

    for(const std::string& input : this->requiredInputNames){
        std::error_code ec;

        logger.msg(Arc::DEBUG, "Moving input '%s'", input);

        fs::path inputPath = fs::path(this->workDir).append(input);
        fs::path newLocation = fs::path(newDir)
                                .append(inputPath.filename().generic_string() + suffix);

        logger.msg(Arc::DEBUG, "Moving/Copying file '%s' to '%s'", inputPath.generic_string(), newDir);

        if(copyInstead)
            fs::copy_file(inputPath, newLocation, ec);
        else
            fs::rename(inputPath, newLocation, ec);

        if (ec.value() != 0){
            logger.msg(Arc::FATAL, "Failed to move the inputs: EC(%d)", ec.value());
            inputsFailed = true;
        }
    }

    this->inputsMoved = ! inputsFailed;

    if (this->inputsMoved){
        logger.msg(Arc::DEBUG, "Creating environment");
        // Create env file for the job
        std::string config;

        for (std::string env : this->envs)
            config += env + "\n";

        std::list<std::string>& execs = this->jld.exec;
        if(execs.size() > 1){
            auto execIter = this->jld.exec.begin();
            execIter++; // Do not look the first value (executable)

            config += "ARGS=\"";
            for( ; execIter != execs.end() ; ++execIter){
                config += *execIter + " ";
            }
            config.pop_back();
            config += "\"\n";
        }

        config += "STDOUT=" + stdout_ + "\n";
        config += "STDERR=" + stderr_ + "\n";

        logger.msg(Arc::DEBUG, "ENV:\n%s", config);
        
        std::ofstream envFile(fs::path(newDir).append("env" + suffix));
        envFile << config;
        envFile.close();
    }
};

//
// JobGroup
//
JobGroup::JobGroup(std::string deleg_id, int max_size) : 
    id(Arc::UUID().substr(24)), //12345678-90ab-cdef-1234- (567890abcdef)
    delegId(deleg_id), 
    maxSize(max_size),
    state(GroupState::STORING)
{
    this->basePath = fs::path("/tmp")
                        .append("groups")
                        .append(id);

    this->inputPath = fs::path(this->basePath)
                        .append("inputs");

    mkdir(this->basePath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    mkdir(this->inputPath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    this->firstJobTime = std::chrono::steady_clock::time_point{};
};

void IFIC::JobGroup::PrepareConfigFile(){
    std::string group_config;

    group_config = "EXECS=( ";

    for(GroupedJob& job : *this){
        logger.msg(Arc::DEBUG, "Checking JDL of job %s", job.id);

        for (std::string part : job.jld.exec)
            logger.msg(Arc::DEBUG, "JDL exec: %s", part);

        std::string arg0 = job.jld.exec.front();
        group_config += arg0 + "_" + job.id + " ";
    }

    group_config += ")\n";

    group_config += "GROUP_ID=" + id + "\n";

    logger.msg(Arc::DEBUG, "Config info:\n%s", group_config);

    std::ofstream file(fs::path(this->basePath).append("groupConfig"));
    file << group_config;
    file.close();
};

bool IFIC::JobGroup::HasJobFinished(ARex::GMJobRef& jobRef){
    logger.msg(Arc::DEBUG, "[%s] Checking if job (%s) has finished", this->id, jobRef->get_id());
    
    if(this->state == GroupState::READY || this->state == GroupState::STORING)
        return false;

    if(this->state == GroupState::FINISHED)
        return true;

    fs::path jobStdOut = fs::path(jobRef->SessionDir())
                            .parent_path()
                            .append(this->arcId)
                            .append(jobRef->get_id())
                            .append("DONE");

    return fs::exists(jobStdOut);
};

void IFIC::JobGroup::ReturnJobOutput(ARex::GMJobRef& jobRef){
    if(this->state == GroupState::READY || this->state == GroupState::STORING)
        return;

    logger.msg(Arc::DEBUG, "[%s] Returning outputs of job (%s)", this->id, jobRef->get_id());

    // Extra files created by the grouping manager
    static const std::string invalidFiles[2] = {"env", "DONE"};

    fs::path jobGroupDir = fs::path(jobRef->SessionDir())
                            .parent_path()
                            .append(this->arcId)
                            .append(jobRef->get_id());

    fs::path jobSessionDir = fs::path(jobRef->SessionDir());
        
    logger.msg(Arc::DEBUG, "Cleaning base dir");
    for (fs::directory_entry entry : fs::directory_iterator(jobSessionDir))
        std::remove(entry.path().c_str());

    logger.msg(Arc::DEBUG, "Moving Outputs");

    for (fs::directory_entry entry : fs::directory_iterator(jobGroupDir)){
        std::string filename = entry.path().filename().generic_string();

        // Do not move unnecesary files
        if (std::find(std::begin(invalidFiles), std::end(invalidFiles), filename) != std::end(invalidFiles))
            continue;
        
        fs::rename(entry.path(), fs::path(jobSessionDir).append(filename));
    };
    
    logger.msg(Arc::DEBUG, "Storing finished job");

    this->outputsMoved++;
    if (this->outputsMoved == this->size())
        this->state = GroupState::FINISHED;
};

// void JobGroup::SendGroup(ARex::JobsList* jobList, const ARex::GMConfig& config){
//     std::string uuid = Arc::UUID();
//     size_t pos = Arc::UUID().find_last_of("-");
//     ARex::JobId id = uuid.substr(pos+1);
//     logger.msg(Arc::DEBUG, "Group job id: %s", id);

//     Arc::User owner = Arc::User(0); // TODO: SET OWNER

//     std::string sessionDir = config.SessionRoot("") + '/' + id;

//     ARex::GMJobRef job(new ARex::GMJob(id, owner, sessionDir, ARex::JOB_STATE_ACCEPTED));
    
//     logger.msg(Arc::DEBUG, "Setting up local desc");

//     ARex::JobLocalDescription* local = new ARex::JobLocalDescription();

//     local->jobid = id;
//     local->DN = senderDn;
//     local->lrms = config.DefaultLRMS();  // TODO: NEEDED??
//     local->queue = this->front().jld.queue;
//     local->exec.push_back("/bin/true"); // TODO: THIS HAS TO BE THE EXEC THAT LAUNCHES THE MERGED JOBS
//     local->sessiondir = sessionDir;
    
//     logger.msg(Arc::DEBUG, "Setting inputs and outputs");
//     local->inputdata = std::list<ARex::FileData>();
//     local->outputdata = std::list<ARex::FileData>();

//     // !!!! CRITICAL !!!!
//     // TODO: CHECK REST OF LocalDescription PARAMETERS (SUCH AS RTEs)

//     logger.msg(Arc::DEBUG, "Merging jlds");
//     for (GroupedJob job : *this){
//         if (! job.jld.inputdata.empty())
//             local->inputdata.insert(
//                 job.jld.inputdata.end(), 
//                 job.jld.inputdata.begin(), 
//                 job.jld.inputdata.end());

//         if (! job.jld.outputdata.empty())
//             local->outputdata.insert(
//                 local->outputdata.end(), 
//                 job.jld.outputdata.begin(), 
//                 job.jld.outputdata.end());
//     }

//     logger.msg(Arc::DEBUG, "Obtaining delegation");
//     ARex::DelegationStores* delegs = config.GetDelegations();
//     if (!delegs) {
//         logger.msg(Arc::FATAL, "Failed to obtain delegations");
//         return;
//     }

//     std::string deleg_id = GetDelegationId(config, senderPEMLocation, senderDn);

//     if (deleg_id == "") {
//         return;
//     }

//     local->delegationid = deleg_id;
    

//     if (! config.JobGroups(name)->dry_run) {
//         // std::error_code e;

//         // std::filesystem::create_directory(sessionDir, e);
//         // if (e.value() != 0)
//         //     logger.msg(Arc::ERROR, "Failed to create job directory, error: %s", e.value());
        

//         // std::filesystem::create_directory(config.ControlDir() + id, e);
//         // if (e.value() != 0)
//         //     logger.msg(Arc::ERROR, "Failed to create job directory, error: %s", e.value());
        
//         // ARex::job_local_write_file(*job, config, *local);

//         // fs::path baseDir(local->sessiondir);
//         // fs::path emptyFile("GROUPED");
//         // std::ofstream file(baseDir / emptyFile);

//         // logger.msg(Arc::INFO, "Requesting attention of group '%s'", name);
//         // jobList->RequestAttention(job);
//     }
//     else {
//         logger.msg(
//             Arc::INFO, "DryRun activated for group %s", 
//             name
//         );
//         logger.msg(
//             Arc::INFO, 
//             "Would have sent job (%s) with delegationId (%s) to queue (%s)",
//             id, local->delegationid, local->queue 
//         );
//         logger.msg(
//             Arc::INFO, 
//             "Job (%s) has '%s' as executable, '%s' as inputs and '%s' as outputs",
//             id,
//             printList<std::string>(
//                 local->exec, 
//                 [](const std::string& c) -> const std::string&{ return c; }),
//             printList<ARex::FileData>(
//                 local->inputdata, 
//                 [](const ARex::FileData& c) -> const std::string& { return c.lfn; }),
//             printList<ARex::FileData>(
//                 local->outputdata, 
//                 [](const ARex::FileData& c) -> const std::string& { return c.lfn; })
//         );
//     }

//     firstJobTime = std::chrono::steady_clock::time_point{};
// };

void JobGroup::SendExec(){
    // Remove Jobs that has inputs that were not moved
    //  This could occur on job group timeout
    auto it = this->begin();
    while(it != this->end()){
        if(! (*it).AreInputsReady()){
            logger.msg(Arc::WARNING, "Removing job '%s' from group '%s'.\nReason: Not all inputs downloaded", (*it).id, this->id);
            JobGroups::ReportDeletion((*it).id);
            it = this->erase(it);
        }
        else
            it++;
    }   

    logger.msg(Arc::DEBUG, "Moving inputs of group %s", this->id);
    for (GroupedJob& job : *this)
        job.MoveInputs(this->inputPath.generic_string());

    logger.msg(Arc::DEBUG, "Preparing to send group %s", this->id);
    
    // General Info
    this->PrepareConfigFile();

    logger.msg(Arc::VERBOSE, "Building XLRS");

    logger.msg(Arc::DEBUG, "Job Descriptions of Group");

    for (GroupedJob& job : *this)
        logger.msg(Arc::DEBUG, ParseLocalDescription(&job.jld));
    
    logger.msg(Arc::DEBUG, "> Building general info");

    std::string desc_str =
        "&(executable=run_group.sh)\n"
        "(jobname=grouped_" + getId() + ")\n"
        "(stdout=std.out)\n"
        "(stderr=std.err)\n";

    // Inputs
    logger.msg(Arc::DEBUG, "> Building input files");
    desc_str += "(inputfiles=\n";
    for (GroupedJob& job : *this)
        for(ARex::FileData file : job.jld.inputdata)
            desc_str += BuildFileDesc(file);

    for (auto entry : fs::directory_iterator(this->inputPath)){
        if (entry.is_regular_file()){
            fs::path input = entry.path();

            if (! input.is_absolute())
                input = fs::absolute(input);

            desc_str += "(" + input.filename().generic_string() + " \"" + input.generic_string() + "\")\n";
        }
    }

    desc_str += "(groupConfig \"" + fs::path(basePath).append("groupConfig").generic_string() + "\")\n";
    desc_str += "(GROUPED \"/arc/GROUPED\")";
    desc_str += "(run_group.sh \"/arc/run_group.sh\"))\n";

    // Outputs
    logger.msg(Arc::DEBUG, "> Building output files");
    desc_str += "(outputfiles=\n";

    for (GroupedJob& job : *this) {
        for(ARex::FileData file : job.jld.outputdata)
            desc_str += BuildFileDesc(file);

        for(std::string output : job.outputs)
            desc_str += "(\"/" + job.id + output + "\" \"\")";
    }

    desc_str += "(\"/\" \"\" ))\n";

    // Rest
    logger.msg(Arc::DEBUG, "> Building final info");
    desc_str += 
        // "(executables=workloadExec.sh)\n" // ?????
        "(queue=" + this->front().jld.queue + ")\n"
        "(count=" + std::to_string(this->size()) + ")" // Number of CPU Cores to ask
        "(delegationid=" + this->delegId + ")\n";

    fs::path xlrsPath = fs::path(this->basePath).append("group_" + this->getId() + ".xlrs");

    logger.msg(Arc::DEBUG, "Saving group XLRS at %s", xlrsPath.generic_string());

    std::ofstream stream(xlrsPath.generic_string());
    stream << desc_str;
    stream.close();

    logger.msg(Arc::DEBUG, "Launching execlp");

    int stdfd[2];
    char output[4096];
    if (pipe(stdfd) == -1)
        throw std::runtime_error("Failed to open pipe");

    __pid_t pid = fork();

    if (pid < 0)
        throw std::exception();

    if (pid == 0){
        dup2 (stdfd[1], STDOUT_FILENO);
        close(stdfd[0]);
        close(stdfd[1]);

        std::string hostname = getenv("HOSTNAME");

        execlp(
            "arcsub", 
            "arcsub",
            "-d", 
            "FATAL", 
            "-C", 
            hostname.c_str(), 
            "-T", 
            "arcrest", 
            "-Q", 
            "None", 
            xlrsPath.c_str(), 
            NULL
        );
        logger.msg(Arc::DEBUG, "CHILD DONE");
        exit(0);
    }

    close(stdfd[1]);

    wait(NULL); // Wait for child to finish

    int nbytes = read(stdfd[0], output, sizeof(output));
    std::string result = std::string(output).substr(0, nbytes);
    result.pop_back();

    // Result should be something like:
    //  Job submitted with jobid: https://{host}/arex/rest/1.0/jobs/{jobid}
    logger.msg(Arc::DEBUG, "Result:\n%s", result);
    
    size_type last_slash = result.find_last_of('/');

    this->arcId = result.substr(last_slash+1);
    logger.msg(Arc::DEBUG, "ArcId:%s", this->arcId);

    this->state = GroupState::SENT;
};

void JobGroup::AddToGroup(ARex::GMJobRef& jobRef, const std::string controlDir){
    logger.msg(Arc::DEBUG, "Group '%s' has %d jobs", this->id, this->size());

    // Store the Job
    GroupedJob job(jobRef, controlDir);

    logger.msg(Arc::INFO, "Adding job '%s' to '%s'", job.id, this->id);
    this->push_back(job);

    // Check last timestamp
    if (this->firstJobTime == std::chrono::steady_clock::time_point{})
        this->firstJobTime = std::chrono::steady_clock::now();
};

long JobGroup::GetTimeElapsed(){
    std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::seconds>(now - this->firstJobTime).count();
};

bool JobGroup::CheckInputsReady(){
    bool inputsReady = true;

    logger.msg(Arc::DEBUG, "Cheking the status of the inputs of group %s", this->id);
    
    for(GroupedJob& job : *this){
        inputsReady &= job.AreInputsReady();

        if (! inputsReady)
            return false;
    }

    return true;
};

void JobGroup::SetReady() {
    // Can't go back 
    // Storing -> [Ready] -> Sent -> Finished
    if(this->state != GroupState::STORING)
        return;

    logger.msg(Arc::DEBUG, "Setting group '%s' to READY", this->id);
    this->state = GroupState::READY;
};

//
// JobGroupDef
//
JobGroupDef::JobGroupDef(const std::string& name, const IFIC::GroupConfig& groupConfig, const ARex::GMConfig& arexConfig):
    name(name),
    checker(JLDChecker(groupConfig.by, groupConfig.condition, groupConfig.check_value)),
    maxJobs(groupConfig.max_jobs),
    maxSeconds(groupConfig.max_seconds),
    senderDn(groupConfig.senderDn),
    senderPEMLocation(groupConfig.senderPEMLocation)
{this->CreateNewGroup(arexConfig);};

JobGroup* JobGroupDef::AddToGroupDef(ARex::GMJobRef& jobRef, const ARex::GMConfig& config){
    if (! checker.isValid(*jobRef->GetLocalDescription())){
        logger.msg(Arc::DEBUG, "Group '%s' failed JLD check", this->name);
        return nullptr;
    }

    for (JobGroup group : this->groups)
        logger.msg(Arc::DEBUG, "This def has groupid: %s", group.getId());

    logger.msg(Arc::DEBUG, "Checking if job %s it's already in a group", jobRef->get_id());
    JobGroup* group = JobGroups::GetGroupOfJob(jobRef);

    // Is not in a group already
    if(group == nullptr){
        logger.msg(Arc::DEBUG, "Adding %s to a group", jobRef->get_id());

        if(this->groups.back().getState() != JobGroup::GroupState::STORING){
            logger.msg(Arc::DEBUG, "Creating a new group");
            group = this->CreateNewGroup(config);
            if(group == nullptr){
                logger.msg(Arc::FATAL, "Failed to create a new group");
                throw std::exception();
            }
        }
        else
            group = &this->groups.back();

        std::string controlDir = ARex::job_control_path(config.ControlDir(), jobRef->get_id(), NULL);
        logger.msg(Arc::DEBUG, "Adding it to the group %s", group->getId());
        group->AddToGroup(jobRef, controlDir);
        logger.msg(Arc::DEBUG, "Job Store success");
    }

    // Check if it can be sent
    logger.msg(Arc::DEBUG, "Checking send status");
    if((group->size() == this->maxJobs && group->CheckInputsReady()) || group->GetTimeElapsed() > this->maxSeconds)
        group->SetReady();

    JobGroups::jobToGroup[jobRef->get_id()] = group;
    return group;
};

JobGroup* JobGroupDef::CreateNewGroup(const ARex::GMConfig& config){
    std::string deleg_id = GetDelegationId(config, this->senderPEMLocation, this->senderDn);  

    if (deleg_id == "") 
        return nullptr;

    JobGroup* group = new JobGroup(deleg_id, this->maxJobs);
    this->groups.push_back(*group);

    return group;
};

//
// JobGroups
//
void JobGroups::BuildGroups(const ARex::GMConfig& config){
    // Only build once
    if(!JobGroups::groups.empty())
        return;

    // Initialize the group directory
    int res = mkdir("/tmp/groups", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    int err = errno;

    if (res != 0 && err != EEXIST){
        logger.msg(Arc::FATAL, "Error while creating directory '/tmp/groups': %s", err);
        throw std::exception();
    }

    // Build the groups
    for(std::string name : config.JobGroupNames()){
        logger.msg(Arc::DEBUG, "No groups found with name '%s', creating one", name);
        JobGroups::groups.insert(std::pair(
            name,
            new JobGroupDef(
                name,
                *config.JobGroups(name),
                config
            )
        ));
    }
};

JobGroups::GroupedStatus JobGroups::TryAddJob(ARex::JobsList* jobList, ARex::GMJobRef& jobRef, const ARex::GMConfig& config){
    // Build the groups if not done already
    JobGroups::BuildGroups(config);

    std::string jobCtrl = ARex::job_control_path(config.ControlDir(), jobRef->get_id(), NULL);
    std::string jobId = jobRef->get_id();

    logger.msg(Arc::DEBUG, "Job to try to group (%s) has status (%s)", jobId, jobRef->get_state_name());

    // Check if it is a group job
    if (IsGroupedInGrami(jobCtrl)){
        logger.msg(Arc::VERBOSE, "The job is a group, won't try to add it");
        return JobGroups::GroupedStatus::NOT_PROCESSED;
    }

    // Try to add the job onto a group
    for (std::map<std::string, JobGroupDef*>::iterator iter = JobGroups::groups.begin(); iter != JobGroups::groups.end(); ++iter){
        // Add it to the group
        JobGroup* group = iter->second->AddToGroupDef(jobRef, config);

        if (group == nullptr){
            logger.msg(Arc::DEBUG, "Job group not stored at %s", iter->first);
            continue; // Try on another group
        }

        JobGroup::GroupState state = group->getState();

        if(state == JobGroup::GroupState::SENT || state == JobGroup::GroupState::FINISHED){
            if (group->HasJobFinished(jobRef)){
                group->ReturnJobOutput(jobRef);
                return JobGroups::GroupedStatus::FINISHING;
            }
        }

        if (state == JobGroup::GroupState::READY){
            logger.msg(Arc::INFO, "Job group '%s' ready to be sent", iter->first);
            group->SendExec();
        }
        else if (state == JobGroup::GroupState::STORING){
            logger.msg(Arc::DEBUG, "The job stored in a group given by '%s'", iter->first);
        }

        return JobGroups::GroupedStatus::STORED;
    }

    logger.msg(Arc::VERBOSE, "No valid groups to store the job at");
    return JobGroups::GroupedStatus::NOT_PROCESSED;
};

JobGroup* JobGroups::GetGroupOfJob(ARex::GMJobRef& jobRef){
    try{
        return JobGroups::jobToGroup.at(jobRef->get_id());
    }
    catch(std::out_of_range){
        return nullptr;
    }
};

void JobGroups::ReportDeletion(ARex::JobId jobId){
    JobGroups::jobToGroup.erase(jobId);
};

}

//
// GMConfig
//
const IFIC::GroupConfig* ARex::GMConfig::JobGroups(const std::string groupName) const {
  std::map<std::string, IFIC::GroupConfig>::const_iterator iter = this->job_groups.find(groupName);
  
  if (iter == this->job_groups.end()){
    logger.msg(Arc::WARNING, "Unable to find group '%s'", groupName);
    return nullptr;
  }

  return &(iter->second);
};

const std::list<std::string> ARex::GMConfig::JobGroupNames() const {
    std::list<std::string> names = std::list<std::string>();

    for (std::pair<const std::string, IFIC::GroupConfig> pair : this->job_groups){
        names.push_front(pair.first);
    }

    return names;
};
