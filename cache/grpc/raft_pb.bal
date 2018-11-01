import ballerina/grpc;
import ballerina/io;

public type raftBlockingStub object {
    public grpc:Client clientEndpoint;
    public grpc:Stub stub;

    function initStub (grpc:Client ep) {
        grpc:Stub navStub = new;
        navStub.initStub(ep, "blocking", DESCRIPTOR_KEY, descriptorMap);
        self.stub = navStub;
    }
    
    function voteResponseRPC (VoteRequest req, grpc:Headers? headers = ()) returns ((VoteResponse, grpc:Headers)|error) {
        
        var unionResp = self.stub.blockingExecute("raft/voteResponseRPC", req, headers = headers);
        match unionResp {
            error payloadError => {
                return payloadError;
            }
            (any, grpc:Headers) payload => {
                grpc:Headers resHeaders;
                any result;
                (result, resHeaders) = payload;
                return (check <VoteResponse>result, resHeaders);
            }
        }
    }
    
    function appendEntriesRPC (AppendEntries req, grpc:Headers? headers = ()) returns ((AppendEntriesResponse, grpc:Headers)|error) {
        
        var unionResp = self.stub.blockingExecute("raft/appendEntriesRPC", req, headers = headers);
        match unionResp {
            error payloadError => {
                return payloadError;
            }
            (any, grpc:Headers) payload => {
                grpc:Headers resHeaders;
                any result;
                (result, resHeaders) = payload;
                return (check <AppendEntriesResponse>result, resHeaders);
            }
        }
    }
    
    function addServerRPC (string req, grpc:Headers? headers = ()) returns ((ConfigChangeResponse, grpc:Headers)|error) {
        
        var unionResp = self.stub.blockingExecute("raft/addServerRPC", req, headers = headers);
        match unionResp {
            error payloadError => {
                return payloadError;
            }
            (any, grpc:Headers) payload => {
                grpc:Headers resHeaders;
                any result;
                (result, resHeaders) = payload;
                return (check <ConfigChangeResponse>result, resHeaders);
            }
        }
    }
    
    function clientRequestRPC (string req, grpc:Headers? headers = ()) returns ((ConfigChangeResponse, grpc:Headers)|error) {
        
        var unionResp = self.stub.blockingExecute("raft/clientRequestRPC", req, headers = headers);
        match unionResp {
            error payloadError => {
                return payloadError;
            }
            (any, grpc:Headers) payload => {
                grpc:Headers resHeaders;
                any result;
                (result, resHeaders) = payload;
                return (check <ConfigChangeResponse>result, resHeaders);
            }
        }
    }
    
};

public type raftStub object {
    public grpc:Client clientEndpoint;
    public grpc:Stub stub;

    function initStub (grpc:Client ep) {
        grpc:Stub navStub = new;
        navStub.initStub(ep, "non-blocking", DESCRIPTOR_KEY, descriptorMap);
        self.stub = navStub;
    }
    
    function voteResponseRPC (VoteRequest req, typedesc listener, grpc:Headers? headers = ()) returns (error?) {
        
        return self.stub.nonBlockingExecute("raft/voteResponseRPC", req, listener, headers = headers);
    }
    
    function appendEntriesRPC (AppendEntries req, typedesc listener, grpc:Headers? headers = ()) returns (error?) {
        
        return self.stub.nonBlockingExecute("raft/appendEntriesRPC", req, listener, headers = headers);
    }
    
    function addServerRPC (string req, typedesc listener, grpc:Headers? headers = ()) returns (error?) {
        
        return self.stub.nonBlockingExecute("raft/addServerRPC", req, listener, headers = headers);
    }
    
    function clientRequestRPC (string req, typedesc listener, grpc:Headers? headers = ()) returns (error?) {
        
        return self.stub.nonBlockingExecute("raft/clientRequestRPC", req, listener, headers = headers);
    }
    
};


public type raftBlockingClient object {
    public grpc:Client client;
    public raftBlockingStub stub;
    public grpc:ClientEndpointConfig cfg;

    public function init (grpc:ClientEndpointConfig config) {
        // initialize client endpoint.
        grpc:Client c = new;
        c.init(config);
        self.client = c;
        // initialize service stub.
        raftBlockingStub s = new;
        s.initStub(c);
        self.stub = s;
        cfg=config;
    }

    public function getCallerActions () returns raftBlockingStub {
        return self.stub;
    }
};

public type raftClient object {
    public grpc:Client client;
    public raftStub stub;

    public function init (grpc:ClientEndpointConfig config) {
        // initialize client endpoint.
        grpc:Client c = new;
        c.init(config);
        self.client = c;
        // initialize service stub.
        raftStub s = new;
        s.initStub(c);
        self.stub = s;
    }

    public function getCallerActions () returns raftStub {
        return self.stub;
    }
};


type VoteRequest record {
    int term;
    string candidateID;
    int lastLogIndex;
    int lastLogTerm;
    
};

type VoteResponse record {
    boolean granted;
    int term;
    
};

type AppendEntries record {
    int term;
    string leaderID;
    int prevLogIndex;
    int prevLogTerm;
    LogEntry[] entries;
    int leaderCommit;
    
};

type LogEntry record {
    int term;
    string command;
    
};

type AppendEntriesResponse record {
    int term;
    boolean sucess;
    int followerMatchIndex;
    
};

type ConfigChangeResponse record {
    boolean sucess;
    string leaderHint;
    
};


@final string DESCRIPTOR_KEY = "raft.proto";
map descriptorMap = {
"raft.proto":"0A0A726166742E70726F746F1A1E676F6F676C652F70726F746F6275662F77726170706572732E70726F746F2289010A0B566F74655265717565737412120A047465726D18012001280352047465726D12200A0B63616E6469646174654944180220012809520B63616E646964617465494412220A0C6C6173744C6F67496E646578180320012803520C6C6173744C6F67496E64657812200A0B6C6173744C6F675465726D180420012803520B6C6173744C6F675465726D223C0A0C566F7465526573706F6E736512180A076772616E74656418012001280852076772616E74656412120A047465726D18022001280352047465726D22CE010A0D417070656E64456E747269657312120A047465726D18012001280352047465726D121A0A086C6561646572494418022001280952086C6561646572494412220A0C707265764C6F67496E646578180320012803520C707265764C6F67496E64657812200A0B707265764C6F675465726D180420012803520B707265764C6F675465726D12230A07656E747269657318052003280B32092E4C6F67456E7472795207656E747269657312220A0C6C6561646572436F6D6D6974180620012803520C6C6561646572436F6D6D697422380A084C6F67456E74727912120A047465726D18012001280352047465726D12180A07636F6D6D616E641802200128095207636F6D6D616E6422730A15417070656E64456E7472696573526573706F6E736512120A047465726D18012001280352047465726D12160A067375636573731802200128085206737563657373122E0A12666F6C6C6F7765724D61746368496E6465781803200128035212666F6C6C6F7765724D61746368496E646578224E0A14436F6E6669674368616E6765526573706F6E736512160A067375636573731801200128085206737563657373121E0A0A6C656164657248696E74180220012809520A6C656164657248696E743280020A0472616674122E0A0F766F7465526573706F6E7365525043120C2E566F7465526571756573741A0D2E566F7465526573706F6E7365123A0A10617070656E64456E7472696573525043120E2E417070656E64456E74726965731A162E417070656E64456E7472696573526573706F6E736512430A0C616464536572766572525043121C2E676F6F676C652E70726F746F6275662E537472696E6756616C75651A152E436F6E6669674368616E6765526573706F6E736512470A10636C69656E7452657175657374525043121C2E676F6F676C652E70726F746F6275662E537472696E6756616C75651A152E436F6E6669674368616E6765526573706F6E7365620670726F746F33",
"google.protobuf.wrappers.proto":"0A0E77726170706572732E70726F746F120F676F6F676C652E70726F746F62756622230A0B446F75626C6556616C756512140A0576616C7565180120012801520576616C756522220A0A466C6F617456616C756512140A0576616C7565180120012802520576616C756522220A0A496E74363456616C756512140A0576616C7565180120012803520576616C756522230A0B55496E74363456616C756512140A0576616C7565180120012804520576616C756522220A0A496E74333256616C756512140A0576616C7565180120012805520576616C756522230A0B55496E74333256616C756512140A0576616C756518012001280D520576616C756522210A09426F6F6C56616C756512140A0576616C7565180120012808520576616C756522230A0B537472696E6756616C756512140A0576616C7565180120012809520576616C756522220A0A427974657356616C756512140A0576616C756518012001280C520576616C756542570A13636F6D2E676F6F676C652E70726F746F627566420D577261707065727350726F746F50015A057479706573F80101A20203475042AA021E476F6F676C652E50726F746F6275662E57656C6C4B6E6F776E5479706573620670726F746F33"

};
