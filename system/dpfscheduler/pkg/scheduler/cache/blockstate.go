package cache

import (
	"columbia.github.com/privatekube/dpfscheduler/pkg/scheduler/util"
	columbiav1 "columbia.github.com/privatekube/privacyresource/pkg/apis/columbia.github.com/v1"
	"fmt"
	"github.com/mit-drl/goop"
	"github.com/mit-drl/goop/solvers"
	//	"k8s.io/klog"
	"math"
	//	"time"
	"sync"
)

type DemandState struct {
	Availability bool
	Share        float64
	Cost         float64
	IsValid      bool
}

type BlockState struct {
	sync.RWMutex
	block         *columbiav1.PrivateDataBlock
	initialBudget columbiav1.PrivacyBudget
	Demands       map[string]*DemandState
	arrivalNumber int
	id            string
}

func NewBlockState(block *columbiav1.PrivateDataBlock) *BlockState {
	return &BlockState{
		block:         block,
		initialBudget: block.Spec.InitialBudget,
		Demands:       map[string]*DemandState{},
		arrivalNumber: 0,
		id:            util.GetBlockId(block),
	}
}

func (blockState *BlockState) GetReservedMap() map[string]columbiav1.PrivacyBudget {
	blockState.RLock()
	defer blockState.RUnlock()

	reservedMap := blockState.block.Status.ReservedBudgetMap
	return reservedMap
}

func (blockState *BlockState) computeDemandState(budget columbiav1.PrivacyBudget, relval_a map[float64]float64, schedulerName string) *DemandState {
	var initialBudget columbiav1.PrivacyBudget
	initialBudget = blockState.block.Spec.InitialBudget
	availableBudget := blockState.block.Status.AvailableBudget

	share := 0.0
	blockCost := 0.0
	if schedulerName == util.DPF {
		share = getDominantShare(budget, initialBudget)
	} else if schedulerName == util.OVERFLOW_RELEVANCE || schedulerName == util.SOFT_KNAPSACK {
		blockCost = getPerBlockCost(budget, availableBudget, relval_a, schedulerName)
	}

	return &DemandState{
		Availability: availableBudget.HasEnough(budget),
		Share:        share,
		Cost:         blockCost,
		IsValid:      true,
	}
}

func getPerBlockCost(budget columbiav1.PrivacyBudget, availableBudget columbiav1.PrivacyBudget, relval_a map[float64]float64, schedulerName string) float64 {
	budget.ToRenyi()
	availableBudget.ToRenyi()
	return getPerBlockCostRenyi(budget.Renyi, availableBudget.Renyi, relval_a, schedulerName)
}

func getPerBlockCostRenyi(budget columbiav1.RenyiBudget, availableBudget columbiav1.RenyiBudget, relval_a map[float64]float64, schedulerName string) float64 {
	blockCost := 0.0
	b, _ := columbiav1.ReduceToSameSupport(budget, availableBudget)
	for i := range b {
		relval := relval_a[b[i].Alpha] //a[i].Epsilon
		if schedulerName == util.OVERFLOW_RELEVANCE {
			if relval > 0 {
				blockCost += b[i].Epsilon / relval
			} else {
				blockCost = 0
				break
			}
		} else if schedulerName == util.SOFT_KNAPSACK {
			blockCost += b[i].Epsilon * relval
		}
	}
	return blockCost
}

func getDominantShare(budget columbiav1.PrivacyBudget, base columbiav1.PrivacyBudget) float64 {
	var share float64

	if budget.IsEpsDelType() && base.IsEpsDelType() {
		if base.EpsDel.Epsilon > columbiav1.EmptyThreshold {
			share = math.Max(share, budget.EpsDel.Epsilon/base.EpsDel.Epsilon)
		}

		if base.EpsDel.Delta > columbiav1.EmptyThreshold {
			share = math.Max(share, budget.EpsDel.Delta/base.EpsDel.Delta)
		}

		return share
	}

	budget.ToRenyi()
	base.ToRenyi()

	return getRenyiDominantShare(budget.Renyi, base.Renyi)
}

func getRenyiDominantShare(budget columbiav1.RenyiBudget, base columbiav1.RenyiBudget) float64 {
	var share float64
	b, c := columbiav1.ReduceToSameSupport(budget, base)
	for i := range b {
		share = math.Max(share, b[i].Epsilon/c[i].Epsilon)
	}
	return share
}

func oldgetRenyiDominantShare(budget columbiav1.RenyiBudget, base columbiav1.RenyiBudget) float64 {
	var share float64

	p := 0
	for q := range base {
		for p < len(budget) && budget[p].Alpha < base[q].Alpha {
			p++
		}

		if budget[q].Epsilon <= 0 {
			continue
		}

		if p < len(budget) && budget[p].Alpha == base[q].Alpha {
			share = math.Max(share, budget[p].Epsilon/base[q].Epsilon)
		}
	}

	return share
}

func (blockState *BlockState) compute_block_overflow() map[float64]float64 {
	overflow_a := map[float64]float64{}

	for _, reservedBudget := range blockState.block.Status.ReservedBudgetMap {
		reservedBudget.ToRenyi()
		blockState.block.Status.AvailableBudget.ToRenyi()
		r, a := columbiav1.ReduceToSameSupport(reservedBudget.Renyi, blockState.block.Status.AvailableBudget.Renyi)
		for i := range r {
			alpha := r[i].Alpha
			if _, ok := overflow_a[alpha]; !ok {
				overflow_a[alpha] = -a[i].Epsilon
			}
			overflow_a[alpha] += r[i].Epsilon
		}
	}
	return overflow_a
}

// Softmax returns the softmax of m with temperature T.
// i.e. exp(x / T) / sum(exp(x / T)) in vector form
func Softmax(x []float64, T float64) []float64 {
	r := make([]float64, len(x))
	d := 1e-15 // Denominator, don't divide by zero

	// Substract the max to avoid overflow
	m := 0.0
	for i, v := range x {
		if i == 0 || v > m {
			m = v
		}
	}
	// Denominator
	for _, v := range x {
		d += math.Exp((v - m) / T)
	}
	// Softmax vector
	for i, v := range x {
		r[i] = math.Exp((v-m)/T) / d
	}
	return r
}

func Argmax(x []float64) []float64 {
	r := make([]float64, len(x))
	max_v := 0.0
	max_i := 0
	for i, v := range x {
		r[i] = 0.0
		if v > max_v {
			max_v = v
			max_i = i
		}
	}
	r[max_i] = max_v
	return r
}

func gurobi_solve(demands_per_alpha []float64, priorities_per_alpha []float64, a float64) float64 {
	var mba float64
	// Instantiate a new model
	m := goop.NewModel()
	// Add your variables to the model
	x := m.AddBinaryVarVector(len(demands_per_alpha))
	// Add your constraints

	exp := goop.NewLinearExpr(0)
	for i := 0; i < len(demands_per_alpha); i++ {
		exp.Plus(x[i].Mult(demands_per_alpha[i]))
	}
	const_expr := goop.NewLinearExpr(a)
	m.AddConstr(exp.LessEq(const_expr))

	// Set a linear objective using your variables
	obj := goop.NewLinearExpr(0)
	for i := 0; i < len(priorities_per_alpha); i++ {
		obj.Plus(x[i].Mult(float64(priorities_per_alpha[i])))
	}
	m.SetObjective(obj, goop.SenseMaximize)
	// Optimize the variables according to the model
	sol, err := m.Optimize(solvers.NewGurobiSolver())

	if err != nil {
		panic("Should not have an error")
	}

	// Print out the solution
	//      fmt.Println("x =", sol.Value(x))
	mba = 0.0
	for i := 0; i < len(priorities_per_alpha); i++ {
		if sol.Value(x[i]) > 0 {
			mba += float64(priorities_per_alpha[i])
		}
	}
	return mba
}

func (blockState *BlockState) compute_knapsack(claimCache ClaimCache) map[float64]float64 {
	knapsack_a := map[float64]float64{}
	demands_per_alpha := map[float64][]float64{}
	priorities_per_alpha := map[float64][]float64{}

	for claim_id, reservedBudget := range blockState.block.Status.ReservedBudgetMap {
		reservedBudget.ToRenyi()
		blockState.block.Status.AvailableBudget.ToRenyi()
		r, _ := columbiav1.ReduceToSameSupport(reservedBudget.Renyi, blockState.block.Status.AvailableBudget.Renyi)
		for i := range r {
			alpha := r[i].Alpha
			if _, ok := demands_per_alpha[alpha]; !ok {
				demands_per_alpha[alpha] = make([]float64, 0, 16)
				priorities_per_alpha[alpha] = make([]float64, 0, 16)
			}
			// Fill the array with all the demands for this block/alpha
			if r[i].Epsilon > 0 {
				demands_per_alpha[alpha] = append(demands_per_alpha[alpha], r[i].Epsilon)
				priorities_per_alpha[alpha] = append(priorities_per_alpha[alpha], claimCache.Get(claim_id).claim.Spec.Profit)
			}
		}
	}
	a := blockState.block.Status.AvailableBudget.Renyi
	var wg sync.WaitGroup
	wg.Add(len(a))
	var tmp = make([]float64, len(a))

	for i := range a {
		alpha := a[i].Alpha
		go func(alpha float64, i int) {
			defer wg.Done()

			if len(demands_per_alpha[alpha]) > 0 && a[i].Epsilon > 0 {
				tmp[i] = gurobi_solve(demands_per_alpha[alpha], priorities_per_alpha[alpha], a[i].Epsilon) / a[i].Epsilon
			} else {
				tmp[i] = 0
			}
		}(alpha, i)
	}
	wg.Wait()

	//r := Softmax(tmp, 10000)
	r := Argmax(tmp)
	// Quick and sloppy conversion back to a map
	i := 0
	for k := range knapsack_a {
		knapsack_a[k] = r[i]
		i++
	}
	return knapsack_a
}

// UpdateDemandMap updates the demand for each claim on this block
// - cost
// - dominant share
// - availability (enough eps/delta budget, or one alpha positive)
// - valid

func (blockState *BlockState) UpdateDemandMap(claimCache ClaimCache, schedulerName string) map[string]*DemandState {
	blockState.Lock()
	defer blockState.Unlock()

	//	start := time.Now()

	var relval map[float64]float64

	if schedulerName == util.OVERFLOW_RELEVANCE {
		relval = blockState.compute_block_overflow()
	} else if schedulerName == util.SOFT_KNAPSACK {
		relval = blockState.compute_knapsack(claimCache)
	}
	demandMap := map[string]*DemandState{}
	for claimId, reservedBudget := range blockState.block.Status.ReservedBudgetMap {
		demand := blockState.computeDemandState(reservedBudget, relval, schedulerName)
		demandMap[claimId] = demand
	}
	//	time_elapsed := time.Since(start)
	//      fmt.Println("Time elapsed - block- id", blockState.id, time_elapsed, start)
	//	klog.Infof("Time elapsed", blockState.id, time_elapsed)

	// invalid the old demand states
	for _, demandState := range blockState.Demands {
		demandState.IsValid = false
	}
	blockState.Demands = demandMap

	return demandMap
}

func (blockState *BlockState) Snapshot() *columbiav1.PrivateDataBlock {
	blockState.RLock()
	defer blockState.RUnlock()
	if blockState.block == nil {
		return nil
	}
	//klog.Infof("snapshot block: %v. %v", handler.block.Status.CommittedBudgetMap, handler.block.Status.CommittedBudgetMap == nil)
	return blockState.block.DeepCopy()
}

func (blockState *BlockState) View() *columbiav1.PrivateDataBlock {
	blockState.RLock()
	defer blockState.RUnlock()
	return blockState.block
}

func (blockState *BlockState) Update(block *columbiav1.PrivateDataBlock) error {
	blockState.Lock()
	defer blockState.Unlock()
	if block.Generation < blockState.block.Generation {
		return fmt.Errorf("block [%v] to update is staled", block.Name)
	}
	//klog.Infof("update block: %v. %v", handler.block.Status.CommittedBudgetMap, handler.block.Status.CommittedBudgetMap == nil)
	blockState.block = block
	return nil
}

func (blockState *BlockState) UpdateReturnOld(block *columbiav1.PrivateDataBlock) (*columbiav1.PrivateDataBlock, error) {
	blockState.Lock()
	defer blockState.Unlock()
	old := blockState.block
	if block.Generation < old.Generation {
		return old, fmt.Errorf("block [%v] to update is staled", block.Name)
	}
	//klog.Infof("update block: %v. %v", handler.block.Status.CommittedBudgetMap, handler.block.Status.CommittedBudgetMap == nil)
	blockState.block = block
	return old, nil
}

func (blockState *BlockState) Delete() {
	blockState.Lock()
	defer blockState.Unlock()
	blockState.block = nil
}

func (blockState *BlockState) IsLive() bool {
	blockState.RLock()
	defer blockState.RUnlock()
	return blockState.block == nil
}

func (blockState *BlockState) GetId() string {
	return blockState.id
}

func (blockState *BlockState) Quota(K int) columbiav1.PrivacyBudget {
	return blockState.initialBudget.Div(float64(K))
}
