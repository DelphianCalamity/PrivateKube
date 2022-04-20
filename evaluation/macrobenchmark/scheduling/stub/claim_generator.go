package stub

import (
	"fmt"
	"log"
	"math/rand"
	"sort"
	"time"

	columbiav1 "columbia.github.com/privatekube/privacyresource/pkg/apis/columbia.github.com/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type PipelineSampler interface {
	Next() Pipeline
	Size() int
	//SimulationTime() time.Duration
}

type ClaimGenerator struct {
	BlockGen              *BlockGenerator
	Pipelines             PipelineSampler
	MeanPipelinesPerBlock float64
	Rand                  *rand.Rand
}

// Alibaba workload

type AlibabaSampler struct {
	Current   int
	Pipelines []Pipeline
}

func MakeAlibabaSampler(workload_dir string) *AlibabaSampler {
	return &AlibabaSampler{Current: -1, Pipelines: ReadTasks(workload_dir)}
}

func (p *AlibabaSampler) Next() Pipeline {
	p.Current++
	//fmt.Println("Next:", p.Current, p.Pipelines[p.Current])
	return p.Pipelines[p.Current]
}

func (p *AlibabaSampler) Size() int {
	return len(p.Pipelines)
}

// Amazon Reviews Workload

type MiceElphantsSampler struct {
	MiceRatio                float64
	Mice                     []Pipeline
	Elephants                []Pipeline
	Rand                     *rand.Rand
	MeanPipelinesPerBlock    float64
	Nblocks                  int
	BlockIntervalMillisecond int
}

func MakeMiceElphantsSampler(rdp bool, mice_ratio float64, mice_path string, elephants_path string, n_blocks int, mean_pipelines_per_block float64, block_interval_millisecond int) *MiceElphantsSampler {
	mice := LoadDir(mice_path)
	elephants := LoadDir(elephants_path)
	m := make([]Pipeline, 0, len(mice))
	e := make([]Pipeline, 0, len(elephants))
	for name, raw_pipeline := range mice {
		m = append(m, NewPipeline(name, raw_pipeline, rdp, 0))
	}
	for name, raw_pipeline := range elephants {
		e = append(e, NewPipeline(name, raw_pipeline, rdp, 1))
	}
	// make result deterministic
	sort.Slice(m, func(i, j int) bool {
		return m[i].Name < m[j].Name
	})
	sort.Slice(e, func(i, j int) bool {
		return e[i].Name < e[j].Name
	})

	return &MiceElphantsSampler{
		MiceRatio:                mice_ratio,
		Mice:                     m,
		Elephants:                e,
		Rand:                     rand.New(rand.NewSource(100)),
		MeanPipelinesPerBlock:    mean_pipelines_per_block,
		Nblocks:                  n_blocks,
		BlockIntervalMillisecond: block_interval_millisecond,
	}
}

func (p *MiceElphantsSampler) Size() int {
	return p.Nblocks * int(p.MeanPipelinesPerBlock)
}

func (p *MiceElphantsSampler) Next() Pipeline {
	if p.Rand.Float64() < p.MiceRatio {
		i := p.Rand.Intn(len(p.Mice))
		fmt.Println("Sample one mice: \n", i)
		return p.Mice[i]
	}
	i := p.Rand.Intn(len(p.Elephants))
	fmt.Println("Sample one elephants: \n", i)
	return p.Elephants[i]
}

//func (g *ClaimGenerator) SampleProfit(Type int) float64 {
//	i := g.Rand.Intn(4)
//	profit := 1.0
//	scale := 1.0
//	if Type == 1 {
//		scale = 10
//	}
//	switch i {
//	case 0:
//		profit = 50 * scale
//	case 1:
//		profit = 10 * scale
//	case 2:
//		profit = 5 * scale
//	case 3:
//		profit = 1 * scale
//	default:
//		fmt.Println("Invalid priority")
//	}
//
//	return profit
//}

func (g *ClaimGenerator) createClaim(block_index int, model Pipeline, timeout time.Duration) (*columbiav1.PrivacyBudgetClaim, error) {
	// Store the timestamp for analysis
	annotations := make(map[string]string)
	annotations["actualStartTime"] = fmt.Sprint(int(time.Now().UnixNano() / 1_000_000))
	//profit := model.Epsilon * float64(model.NBlocks)
	//profit := g.SampleProfit(model.Type)

	// Create a new claim with flat demand that asks for the NBlock most recent blocks
	claim := &columbiav1.PrivacyBudgetClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d-%s", model.Name, block_index, RandId()),
			Namespace: g.BlockGen.Stub.Namespace,
			// CreationTimestamp: metav1.NewTime(time.Now()),
			Annotations: annotations,
		},
		Spec: columbiav1.PrivacyBudgetClaimSpec{

			Profit: model.Profit,
			Requests: []columbiav1.Request{
				{
					Identifier: "1",
					AllocateRequest: &columbiav1.AllocateRequest{
						Dataset: g.BlockGen.Dataset,
						Conditions: []columbiav1.Condition{
							{
								Attribute:    "startTime",
								Operation:    ">=",
								NumericValue: ToDecimal(block_index - model.NBlocks + 1),
							},
							{
								Attribute:    "startTime",
								Operation:    "<=",
								NumericValue: ToDecimal(block_index),
							},
						},

						MinNumberOfBlocks: model.NBlocks,
						MaxNumberOfBlocks: model.NBlocks,
						ExpectedBudget: columbiav1.BudgetRequest{
							Constant: &model.Demand,
						},
						Timeout: int64(timeout / time.Millisecond),
					},
				},
			},
		},
	}
	// if block_index-model.NBlocks+1 < 0 {
	// 	// Border case where the claim is asking for blocks that don't exist
	// 	// Let's make sure it is not allocated
	// 	claim.Spec.Requests[0].AllocateRequest.Timeout = 0
	// }
	return g.BlockGen.Stub.CreateClaim(claim)
}

func (g *ClaimGenerator) Run() {
	ticker := time.NewTicker(g.BlockGen.BlockInterval)
	go func() {
		index := 0
		for index < g.BlockGen.MaxBlocks {
			<-ticker.C
			go g.BlockGen.createDataBlock(index)
			// go g.createClaim(index, model)
			index++
		}
	}()
}
func (g *ClaimGenerator) RunExponentialDeterministic(claim_names chan string, default_timeout time.Duration, n_blocks int) {
	total_duration := time.Duration(g.BlockGen.MaxBlocks+1) * g.BlockGen.BlockInterval
	//total_duration := time.Duration(n_blocks) * g.BlockGen.BlockInterval
	end_time := g.BlockGen.StartTime.Add(total_duration)
	total_tasks := g.Pipelines.Size()
	r := rand.New(rand.NewSource(100))
	index := 0

	for index < total_tasks {
		interval := (g.Rand.ExpFloat64() / g.MeanPipelinesPerBlock) * float64(g.BlockGen.BlockInterval.Microseconds())
		timer := time.NewTimer(time.Duration(interval) * time.Microsecond)
		<-timer.C

		block_index := g.BlockGen.CurrentIndex()
		// Cap the timeout by the simulation running time (with a five-block margin)
		timeout := time.Until(end_time) + 5*g.BlockGen.BlockInterval
		if timeout > default_timeout {
			timeout = default_timeout
		}
		go func(int, time.Duration, *rand.Rand) {
			pipeline := g.Pipelines.Next()
			claim, err := g.createClaim(block_index, pipeline, timeout)
			if err != nil {
				log.Fatal(err)
			} else {
				claim_names <- claim.ObjectMeta.Name
			}
		}(block_index, timeout, r)
		index++
	}
}

func (g *ClaimGenerator) RunExponential(claim_names chan string, default_timeout time.Duration, n_blocks int) {
	// NOTE: we can try other start/stop strategies
	total_duration := time.Duration(n_blocks+1) * g.BlockGen.BlockInterval
	end_time := g.BlockGen.StartTime.Add(total_duration)
	for time.Since(g.BlockGen.StartTime) < total_duration {
		// The default rate parameter is 1 (so the mean is 1 too)
		interval := (rand.ExpFloat64() / g.MeanPipelinesPerBlock) * float64(g.BlockGen.BlockInterval.Microseconds())
		timer := time.NewTimer(time.Duration(interval) * time.Microsecond)
		<-timer.C
		block_index := g.BlockGen.CurrentIndex()
		// Cap the timeout by the simulation running time (with a five-block margin)
		timeout := time.Until(end_time) + 5*g.BlockGen.BlockInterval
		if timeout > default_timeout {
			timeout = default_timeout
		}
		go func(int, time.Duration) {
			pipeline := g.Pipelines.Next()
			claim, err := g.createClaim(block_index, pipeline, timeout)
			if err != nil {
				log.Fatal(err)
			} else {
				claim_names <- claim.ObjectMeta.Name
			}
		}(block_index, timeout)
	}
}

func (g *ClaimGenerator) RunConstant(claim_names chan string, default_timeout time.Duration, n_blocks int, task_interval time.Duration) {
	total_duration := time.Duration(g.BlockGen.MaxBlocks+1) * g.BlockGen.BlockInterval
	end_time := g.BlockGen.StartTime.Add(total_duration)
	total_tasks := g.Pipelines.Size()
	fmt.Println("task interval\n\n\n\n", task_interval)

	index := 0
	ticker := time.NewTicker(task_interval)
	for index < total_tasks {
		<-ticker.C
		block_index := g.BlockGen.CurrentIndex()
		// Cap the timeout by the simulation running time (with a five-block margin)
		timeout := time.Until(end_time) + 5*g.BlockGen.BlockInterval
		if timeout > default_timeout {
			timeout = default_timeout
		}
		go func(int, time.Duration) {
			pipeline := g.Pipelines.Next()
			claim, err := g.createClaim(block_index, pipeline, timeout)
			if err != nil {
				log.Fatal(err)
			} else {
				claim_names <- claim.ObjectMeta.Name
			}
		}(block_index, timeout)
		index++
	}
}

func (g *ClaimGenerator) RunCustom(claim_names chan string, default_timeout time.Duration, n_blocks int) {
	total_tasks := g.Pipelines.Size()
	total_duration := time.Duration(n_blocks+1) * g.BlockGen.BlockInterval
	end_time := g.BlockGen.StartTime.Add(total_duration)
	fmt.Println("Total_duration", total_duration)
	index := 0
	for index < total_tasks {
		nextPipeline := g.Pipelines.Next()
		interval := nextPipeline.relative_submit_time * float64(n_blocks) * float64(g.BlockGen.BlockInterval.Microseconds())
		task_interval := time.Duration(interval) * time.Microsecond
		fmt.Println("Task number", index, "Sleep_duration", task_interval)
		time.Sleep(task_interval)

		block_index := g.BlockGen.CurrentIndex()
		// Cap the timeout by the simulation running time (with a ten-block margin)
		timeout := time.Until(end_time) + 10*g.BlockGen.BlockInterval
		if timeout > default_timeout {
			timeout = default_timeout
		}
		go func(block_index int, timeout time.Duration, nextPipeline Pipeline) {
			claim, err := g.createClaim(block_index, nextPipeline, timeout)
			if err != nil {
				log.Fatal(err)
			} else {
				claim_names <- claim.ObjectMeta.Name
			}
		}(block_index, timeout, nextPipeline)
		index++
	}
}
