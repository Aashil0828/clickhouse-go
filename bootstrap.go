package main

import (
	"clickhouse/query"
	"context"
)

func main() {

	ctx := context.Background()

	connection, err := query.InitClickhouseDBConnection(ctx)

	if err != nil {

		panic(err)

	}

	defer connection.Close()

	err = query.CreateTable(ctx, connection)

	if err != nil {

		panic(err)

	}

	err = query.InsertData(ctx, connection)

	if err != nil {

		panic(err)

	}

	err = query.GaugeWithoutFilter(ctx, connection)

	if err != nil {

		panic(err)

	}

	err = query.GaugeWithFilter(ctx, connection)

	if err != nil {

		panic(err)

	}

	err = query.GridWithFilter(ctx, connection)

	if err != nil {

		panic(err)

	}

	err = query.GridWithoutFilter(ctx, connection)

	if err != nil {

		panic(err)

	}

	err = query.TopNWithoutFilter(ctx, connection, 5)

	if err != nil {

		panic(err)

	}

	err = query.TopNWithFilter(ctx, connection, 2)

	if err != nil {

		panic(err)

	}

	err = query.HistogramWithFilterWithoutGroupBy(ctx, connection)

	if err != nil {

		panic(err)

	}

	err = query.HistogramWithoutFilterWithoutGroupBy(ctx, connection)

	if err != nil {

		panic(err)

	}

	err = query.HistogramWithFilterWithGroupBy(ctx, connection)

	if err != nil {

		panic(err)

	}

	err = query.HistogramWithoutFilterWithGroupBy(ctx, connection)

	if err != nil {

		panic(err)

	}

}
