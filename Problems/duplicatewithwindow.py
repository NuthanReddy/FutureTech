# Pattern identification: cap duplicates in sorted input -> read/write two-pointer compaction;
# intended invariant: written prefix retains at most two of each value; legacy bounds are unchecked.
class Solution:
    def removeDuplicates(self, nums: List[int]) -> int:
        # 1. Output: Intend to keep at most two copies of each value in place and return their count.
        # 2. Structure: Equal values are adjacent in sorted input, so one read/write scan can track each run.
        # 3. Constraints: Empty input is unsafe here; List is also not imported in this file.
        # 4. Choice: Advance old_index, count repeats, and copy allowed values to new_index.
        # 5. Why it works: The written prefix should preserve up to two copies per run,
        # but skipping the first value and special-casing zero can discard valid copies.
        occurance=0
        new_index=0
        old_index=0
        if nums[new_index] == 0 and nums[old_index] ==0:
            new_index =2
            old_index =2
        while(len(nums)!=old_index+1):
            # both are same and occ <= 2 - move both forward
            # occ > 2 - move old forward
            # both are diff and occ <= 2 - copy old to new
            if nums[old_index]==nums[old_index+1]:
                occurance += 1
                old_index += 1
            else:
                occurance = 1
                old_index += 1
            if occurance <=2:
                nums[new_index] = nums[old_index]
                new_index +=1
        return new_index
