package cache

import (
	"columbia.github.com/privatekube/dpfscheduler/pkg/scheduler/util"
	columbiav1 "columbia.github.com/privatekube/privacyresource/pkg/apis/columbia.github.com/v1"
	"fmt"
	"github.com/mit-drl/goop"
	"math"
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

func (blockState *BlockState) computeDemandState(budget columbiav1.PrivacyBudget, overflow_a map[float64]float64) *DemandState {
	//	initialBudget := blockState.block.Spec.InitialBudget
	availableBudget := blockState.block.Status.AvailableBudget

	share := 0.0
	blockCost := 0.0
	//if util.IsDPF() {
	//	share = getDominantShare(budget, initialBudget)
	//blockCost = 0
	//} else {
	//	share = 0
	blockCost = getPerBlockCost(budget, availableBudget, overflow_a)
	//}

	return &DemandState{
		Availability: availableBudget.HasEnough(budget),
		Share:        share,
		Cost:         blockCost,
		IsValid:      true,
	}
}

func getPerBlockCost(budget columbiav1.PrivacyBudget, availableBudget columbiav1.PrivacyBudget, overflow_a map[float64]float64) float64 {
	budget.ToRenyi()
	availableBudget.ToRenyi()
	return getPerBlockCostRenyi(budget.Renyi, availableBudget.Renyi, overflow_a)
}

func getPerBlockCostRenyi(budget columbiav1.RenyiBudget, availableBudget columbiav1.RenyiBudget, overflow_a map[float64]float64) float64 {
	blockCost := 0.0
	b, _ := columbiav1.ReduceToSameSupport(budget, availableBudget)
	for i := range b {
		overflow := overflow_a[b[i].Alpha] //a[i].Epsilon
		if overflow > 0 {
			blockCost += b[i].Epsilon / overflow
		} else {
			blockCost = 0
			break
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

func gurobi_solve(demands_per_alpha []float64, priorities_per_alpha []int32, a float64) float64 {
	var relval int32
	// Instantiate a new model
	m := goop.NewModel()
	// Add your variables to the model
	x := m.AddBinaryVarVector(len(demands_per_alpha))
	// Add your constraints
	m.AddConstr(goop.Sum(x.Mult(demands_per_alpha)).LessEq(a))
	// Set a linear objective using your variables
	obj := goop.Sum(x.Mult(priorities_per_alpha))
	m.SetObjective(obj, goop.SenseMaximize)
	// Optimize the variables according to the model
	sol, err := m.Optimize(solvers.NewGurobiSolver())

	if err != nil {
		panic("Should not have an error")
	}

	// Print out the solution
	fmt.Println("x =", sol.Value(x))
	relval = 0
	for i := 0; i < len(priorities_per_alpha); i++ {
		if sol.Value(x[i]) {
			relval += priorities_per_alpha[i]
		}
	}
	return float64(relval)
}

func (blockState *BlockState) compute_knapsack(claimCache ClaimCache) map[float64]float64 {
	knapsack_a := map[float64]float64{}
	demands_per_alpha := map[float64][]float64{}
	priorities_per_alpha := map[float64][]int32{}

	for claim_id, reservedBudget := range blockState.block.Status.ReservedBudgetMap {
		reservedBudget.ToRenyi()
		blockState.block.Status.AvailableBudget.ToRenyi()
		r, _ := columbiav1.ReduceToSameSupport(reservedBudget.Renyi, blockState.block.Status.AvailableBudget.Renyi)
		for i := range r {
			alpha := r[i].Alpha
			if _, ok := demands_per_alpha[alpha]; !ok {
				demands_per_alpha[alpha] = make([]float64, 0, 16)
				priorities_per_alpha[alpha] = make([]int32, 0, 16)
			}
			// Fill the array with all the demands for this block/alpha
			if r[i].Epsilon > 0 {
				demands_per_alpha[alpha] = append(demands_per_alpha[alpha], r[i].Epsilon)
				priorities_per_alpha[alpha] = append(priorities_per_alpha[alpha], claimCache.Get(claim_id).claim.Spec.Priority)
			}
		}
	}
	a := blockState.block.Status.AvailableBudget.Renyi
	for i := range a {
		alpha := a[i].Alpha
		knapsack_a[alpha] = gurobi_solve(demands_per_alpha[alpha], priorities_per_alpha[alpha], a[i].Epsilon)
	}
	return knapsack_a
}

// UpdateDemandMap updates the demand for each claim on this block
// - cost
// - dominant share
// - availability (enough eps/delta budget, or one alpha positive)
// - valid

func (blockState *BlockState) UpdateDemandMap(claimCache ClaimCache) map[string]*DemandState {
	blockState.Lock()
	defer blockState.Unlock()

	var relval map[float64]float64
	//var knapsack_a map[float64]float64
	relval = blockState.compute_block_overflow()
	relval = blockState.compute_knapsack(claimCache)

	demandMap := map[string]*DemandState{}
	for claimId, reservedBudget := range blockState.block.Status.ReservedBudgetMap {
		demand := blockState.computeDemandState(reservedBudget, relval)
		demandMap[claimId] = demand
	}

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
