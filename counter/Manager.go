package counter

type Manager interface {
    Range() (int64, int64, error)
}