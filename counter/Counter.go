package counter

type Counter interface {
    Next() (int64, error)
}