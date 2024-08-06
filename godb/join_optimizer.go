package godb

// Estimate the cost of a join j given the cardinalities (card1, card2) and
// estimated costs (cost1, cost2) of the left and right sides of the join,
// respectively.
//
// The cost of the join should be calculated based on the join algorithm (or
// algorithms) that you implemented for Lab 2. It should be a function of the
// amount of data that must be read over the course of the query, as well as the
// number of CPU opertions performed by your join. Assume that the cost of a
// single predicate application is roughly 1.
func EstimateJoinCost(card1 int, card2 int, cost1 float64, cost2 float64) float64 {
	// Hash-join
	// may need to scan rhs multiple times if lhs is larger than JoinBufferSize
	return float64(card1+card2) + cost1 + max(float64(card1)/float64(JoinBufferSize), 1.0)*cost2
}

// Estimate the cardinality of the result of a join between two tables, given
// the join operator, primary key information, and table statistics.
func EstimateJoinCardinality(t1card int, t2card int) int {
	if t1card == 0 || t2card == 0 {
		return 0
	}

	return max(1, max(t1card, t2card))
}

type TableInfo struct {
	name  string  // Name of the table
	stats Stats   // Statistics for the table; may be nil if no stats are available
	sel   float64 // Selectivity of the filters on the table
}

// A JoinNode represents a join between two tables.
type JoinNode struct {
	leftTable TableInfo
	leftField string

	rightTable TableInfo
	rightField string
}

type orderStats struct {
	order []*JoinNode
	cost  float64
	card  int
}

// Given a list of joins, table statistics, and selectivities, return the best
// order in which to join the tables.
//
// selectivity is a map from table aliases to the selectivity of the filters on
// that table. Note that LogicalJoinNodes contain LogicalSelectNodes that define
// tables to join. Inside a LogicalSelectNode, there is both a table name
// (table) and an alias. We may apply different filters to the same base table
// but with different aliases, so the selectivity map contains selectivities for
// a particular alias, not for a base table.
func OrderJoins(joins []*JoinNode) ([]*JoinNode, error) {
	cache := NewTrie[*JoinNode, *orderStats]()
	cache.Set([]*JoinNode{}, &orderStats{
		order: []*JoinNode{},
		cost:  0,
		card:  0,
	})

	for i := 1; i <= len(joins); i++ {
		subsets, err := KSubsetIter(joins, i)
		if err != nil {
			return nil, err
		}

		for set := subsets(); set != nil; set = subsets() {
			var best *orderStats

			without_j := make([]*JoinNode, len(set)-1)
			for j, join := range set {
				copy(without_j, set[:j])
				copy(without_j[j:], set[j+1:])

				best_without_j := cache.Get(without_j)
				if best_without_j == nil {
					continue
				}
				best_with_j := AddJoin(join, best_without_j)
				if best_with_j != nil && (best == nil || best_with_j.cost < best.cost) {
					best = best_with_j
				}
			}

			cache.Set(set, best)
		}
	}

	return cache.Get(joins).order, nil
}

// Return a new LogicalJoinNode with the inner and outer tables swapped.
func (j *JoinNode) Swap() *JoinNode {
	return &JoinNode{
		leftTable:  j.rightTable,
		leftField:  j.rightField,
		rightTable: j.leftTable,
		rightField: j.leftField,
	}
}

// Return true if the given alias is in the list of joins.
func HasTable(joins []*JoinNode, table string) bool {
	for _, j := range joins {
		if j.leftTable.name == table || j.rightTable.name == table {
			return true
		}
	}
	return false
}

// Given a join order and a new join, add the new join to the join order (if
// possible) and return the new join order.
//
// Returns nil if the new join cannot be added to the join order.
func AddJoin(join *JoinNode, order_stats *orderStats) *orderStats {
	var options []*orderStats

	sel_lhs := join.leftTable.sel
	sel_rhs := join.rightTable.sel
	stats_lhs := join.leftTable.stats
	stats_rhs := join.rightTable.stats
	card_lhs := stats_lhs.EstimateCardinality(sel_lhs)
	cost_lhs := stats_lhs.EstimateScanCost()
	card_rhs := stats_rhs.EstimateCardinality(sel_rhs)
	cost_rhs := stats_rhs.EstimateScanCost()

	swapped_join := join.Swap()

	// Is this the first join in the order?
	if len(order_stats.order) == 0 {
		options = append(options,
			&orderStats{
				order: []*JoinNode{join},
				cost:  EstimateJoinCost(card_lhs, card_rhs, cost_lhs, cost_rhs),
				card:  EstimateJoinCardinality(card_lhs, card_rhs),
			})

		options = append(options,
			&orderStats{
				order: []*JoinNode{swapped_join},
				cost:  EstimateJoinCost(card_rhs, card_lhs, cost_rhs, cost_lhs),
				card:  EstimateJoinCardinality(card_rhs, card_lhs),
			})
	} else {
		if HasTable(order_stats.order, join.leftTable.name) {
			options = append(options,
				&orderStats{
					order: append(order_stats.order, join),
					cost:  EstimateJoinCost(order_stats.card, card_rhs, order_stats.cost, cost_rhs),
					card:  EstimateJoinCardinality(order_stats.card, card_rhs),
				})
		}
		if HasTable(order_stats.order, join.rightTable.name) {
			options = append(options,
				&orderStats{
					order: append(order_stats.order, swapped_join),
					cost:  EstimateJoinCost(order_stats.card, card_lhs, order_stats.cost, cost_lhs),
					card:  EstimateJoinCardinality(order_stats.card, card_lhs),
				})
		}
	}

	if len(options) == 0 {
		return nil
	}

	bestCost := options[0].cost
	best := options[0]
	for _, o := range options {
		if o.cost < bestCost {
			bestCost = o.cost
			best = o
		}
	}

	return best
}

