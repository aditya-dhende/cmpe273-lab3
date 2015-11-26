package main  
  
import (  
    
    "fmt"  
    "hash/crc32"  
    "sort"     
    "net/http"
    "encoding/json" 
    "io/ioutil"
)  
   
type hash []uint32  

type Data struct{
    Key int `json:"key"`
    Value string `json:"value"`
}


func (c hash) Len() int {  
    return len(c)  
}  
    
func (c hash) Swap(i, j int) {  
    c[i], c[j] = c[j], c[i]  
}  

func (c hash) Less(i, j int) bool {  
    return c[i] < c[j]  
} 
  
type Node struct {  
    Id       int  
    IP       string    
}  
  
func NewNode(id int, ip string) *Node {  
    return &Node{  
        Id:       id,  
        IP:       ip,  
    }  
}  
  
type ConsistentHash struct {  
    Nodes       map[uint32]Node  
    IsPresent   map[int]bool  
    Circle      hash  
    
}  
  
func Consistent() *ConsistentHash {  
    return &ConsistentHash{  
        Nodes:     make(map[uint32]Node),   
        IsPresent: make(map[int]bool),  
        Circle:      hash{},  
    }  
}  
  
func (c *ConsistentHash) AddNode(node *Node) bool {  
 
    if _, ok := c.IsPresent[node.Id]; ok {  
        return false  
    }  
    str := c.NodeIP(node)  
    c.Nodes[c.GetValue(str)] = *(node)
    c.IsPresent[node.Id] = true  
    c.Sort()  
    return true  
}  
  
func (c *ConsistentHash) Sort() {  
    c.Circle = hash{}  
    for k := range c.Nodes {  
        c.Circle = append(c.Circle, k)  
    }  
    sort.Sort(c.Circle)  
}  
  
func (c *ConsistentHash) NodeIP(node *Node) string {  
    return node.IP 
}  
  
func (c *ConsistentHash) GetValue(key string) uint32 {  
    return crc32.ChecksumIEEE([]byte(key))  
}  
  
func (c *ConsistentHash) Get(key string) Node {  
    hash := c.GetValue(key)  
    i := c.Search(hash)  
    return c.Nodes[c.Circle[i]]  
}  

func (c *ConsistentHash) Search(hash uint32) int {  
    i := sort.Search(len(c.Circle), func(i int) bool {return c.Circle[i] >= hash })  
    if i < len(c.Circle) {  
        if i == len(c.Circle)-1 {  
            return 0  
        } else {  
            return i  
        }  
    } else {  
        return len(c.Circle) - 1  
    }  
}  
  
  

func Get(key string,circle *ConsistentHash){
    var output Data 
    ip := circle.Get(key)
    url := "http://"+ip.IP+"/keys/"+key
    fmt.Println(url)
    response,err:= http.Get(url)
    if err!=nil{
        fmt.Println("Error:",err)
    }else{
        defer response.Body.Close()
        contents,err:= ioutil.ReadAll(response.Body)
        if(err!=nil){
            fmt.Println(err)
        }
        json.Unmarshal(contents,&output)
        result,_:= json.Marshal(output)
        fmt.Println(string(result))
    }
}

func Put(circle *ConsistentHash, str string, input string){
        ip := circle.Get(str)  
        url := "http://"+ip.IP+"/keys/"+str+"/"+input
        fmt.Println(url)
        request,err := http.NewRequest("PUT",url,nil)
        client := &http.Client{}
        resp, err := client.Do(request)
        if err!=nil{
            fmt.Println("Error:",err)
        }else{
            defer resp.Body.Close()
            fmt.Println("Response : 200 OK")
        }  
}

func GetKeys(url string){
     
    var output []Data
    response,err:= http.Get(url)
    if err!=nil{
        fmt.Println("Error:",err)
    }else{
        defer response.Body.Close()
        contents,err:= ioutil.ReadAll(response.Body)
        if(err!=nil){
            fmt.Println(err)
        }
        json.Unmarshal(contents,&output)
        result,_:= json.Marshal(output)
        fmt.Println(string(result))
    }
}
func main() {   
    node := Consistent()      
    node.AddNode(NewNode(0, "127.0.0.1:3000"))
    node.AddNode(NewNode(1, "127.0.0.1:3001"))
    node.AddNode(NewNode(2, "127.0.0.1:3002")) 
    
    fmt.Println("== Put ===")
            Put(node,"1","p")
            Put(node,"2","q")
            Put(node,"3","r")
            Put(node,"4","s")
            Put(node,"5","t")
            Put(node,"6","u")
            Put(node,"7","v")
            Put(node,"8","w")
            Put(node,"9","x")
            Put(node,"10","y")
          
    fmt.Println("===Get===")
             
            Get("1",node)
            Get("2",node)
            Get("3",node)
            Get("4",node)
            Get("5",node)
            Get("6",node)
            Get("7",node)
            Get("8",node)
            Get("9",node)
            Get("10",node)
           
    
    fmt.Println("===Data for 3000 port===")
    GetKeys("http://127.0.0.1:3000/keys")
        
    fmt.Println("===Data for 3001 port===")
    GetKeys("http://127.0.0.1:3001/keys")
           
    fmt.Println("===Data for 3002 port===")
    GetKeys("http://127.0.0.1:3002/keys")
       
    
}  