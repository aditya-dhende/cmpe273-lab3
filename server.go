package main
import  (

        
		"fmt"
		"net/http"
		"strconv"
		"encoding/json"
		"strings"
		"sort"
		"github.com/julienschmidt/httprouter"
)


type data struct{
	Key int	`json:"key"`
	Value string	`json:"value"`
} 


var d1,d2,d3 [] data
var i1,i2,i3 int
type ByKey []data



func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Len() int           { return len(a) }


func GetAll(rw http.ResponseWriter, request *http.Request,p httprouter.Params){
	
	portnumber := strings.Split(request.Host,":")
	if(portnumber[1]=="3000"){
		sort.Sort(ByKey(d1))
		result,_:= json.Marshal(d1)
		fmt.Fprintln(rw,string(result))
	}else if(portnumber[1]=="3001"){
		sort.Sort(ByKey(d2))
		result,_:= json.Marshal(d2)
		fmt.Fprintln(rw,string(result))
	}else{
		sort.Sort(ByKey(d3))
		result,_:= json.Marshal(d3)
		fmt.Fprintln(rw,string(result))
	}
}

func Put(rw http.ResponseWriter, request *http.Request,p httprouter.Params){
	portnumber := strings.Split(request.Host,":")
	key,_ := strconv.Atoi(p.ByName("key_id"))
	if(portnumber[1]=="3000"){
		d1 = append(d1,data{key,p.ByName("value")})
		i1++
	}else if(portnumber[1]=="3001"){
		d2 = append(d2,data{key,p.ByName("value")})
		i2++
	}else{
		d3 = append(d3,data{key,p.ByName("value")})
		i3++
	}	
}

func Get(rw http.ResponseWriter, request *http.Request,p httprouter.Params){	
	out := d1
	ind := i1
	portnumber := strings.Split(request.Host,":")
	if(portnumber[1]=="3001"){
		out = d2 
		ind = i2
	}else if(portnumber[1]=="3002"){
		out = d3
		ind = i3
	}	
	key,_ := strconv.Atoi(p.ByName("key_id"))
	for i:=0 ; i< ind ;i++{
		if(out[i].Key==key){
			result,_:= json.Marshal(out[i])
			fmt.Fprintln(rw,string(result))
		}
	}
}



func main(){
	i1 = 0
	i2 = 0
	i3 = 0
	router := httprouter.New()
    router.GET("/keys",GetAll)
    router.GET("/keys/:key_id",Get)
    router.PUT("/keys/:key_id/:value",Put)
    go http.ListenAndServe(":3000",router)
    go http.ListenAndServe(":3001",router)
    go http.ListenAndServe(":3002",router)
    select {}
    
}