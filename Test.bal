import ballerina/io;

public function main() {
    map <string> a;
    foreach item in 0...5 {
        a[<string> item]= <string> item;
    }
    foreach k,v in a{
        io:println(a.remove("1"));
    }
    io:println(a);
}
