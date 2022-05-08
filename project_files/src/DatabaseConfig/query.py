class Query:
    @staticmethod
    def put_query(TopCount=10, LastDataKey=0) -> str:
        TopCount = TopCount
        LastDataKey = LastDataKey
        query = """set nocount on
                    DECLARE @TopCount int
                    DECLARE @LastDataKey int
                    DECLARE @NewDataKey int
                    DECLARE @continue_processing bit
                    set @TopCount={}
                    set @LastDataKey={}
                    EXECUTE [dbo].[Get_ADP_Job] @TopCount,@LastDataKey,@NewDataKey OUTPUT,@continue_processing OUTPUT
                    """.format(
            TopCount, LastDataKey
        )
        return query
