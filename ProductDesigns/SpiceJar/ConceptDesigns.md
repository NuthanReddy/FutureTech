# Spice Jar: Text Concept Designs

**Status:** Concept drawings. Not manufacturing drawings.
**Basis:** [Engineering findings](Brainstorming.md) and [prototype brief](ProductDesignBrief.md).

## 1. Drawing rules and common basis

Each section shows a different design option.
The drawings are not to scale.
Hidden parts are described in the text.
Dimensions are examples unless stated otherwise.
Do not infer clearances or thread sizes from a drawing.

Use dry ground spices for the first experiment.
Use SS304 for metal food-contact parts.
Use removable food-contact silicone seals.
Keep drive parts and lubricant outside the spice chamber.
Provide a removable glass lid.
Do not claim longer shelf life from the drawings.

```text
Legend

####     Spice
====     Disc or floor
S        Silicone perimeter seal
////     Thread or helical track
G        Linear guide
N        Nut
B        Bearing
V        Controlled air path
```

For a 45 mm cylindrical bore, 100 mL needs approximately 62.9 mm of usable height.
Allow additional height for headspace, the lid, and seal features.
A 50 mm floor stroke leaves approximately 20.5 mL of geometric chamber volume.
These values exclude part displacement and actual spice packing.
Measure final capacity below the fill line.

For rising floors, LOW is the lowest floor position.
For top discs, FULL and PARTLY EMPTY describe the spice quantity.
Adjust moving parts with the outer lid removed.
Do not force a sealed piston against trapped air.

## 2. Option A: Conventional jar in smaller sizes

### Section

```text
       REMOVABLE GLASS LID
      +------------------+
      |                  |
      +---- gasket ------+
      |    Headspace     |
      |##################|
      |##################|   Glass or SS304 body
      |##################|
      +------------------+
          Fixed bottom
```

### Parts and operation

Use a closed-bottom jar, lid, and removable gasket.
Provide 50 mL and 100 mL usable-capacity options.
Use a smaller jar when the stored quantity decreases.
No part moves inside the jar.

The lid gasket is the storage seal.
All food-contact surfaces are directly accessible.
A stainless-steel body needs a label if top-view identification is insufficient.

### Space and limits

There is no drive enclosure.
External size depends on the jar supplier and lid.
This option does not continuously change the chamber volume.
It is the storage and cleaning comparison for all other options.

## 3. Option B: Removable follower disc

### Sections

```text
       FULL                         PARTLY EMPTY

   +--------------+                +--------------+
   |  Glass lid   |                |  Glass lid   |
   +--- gasket ---+                +--- gasket ---+
   |     tab      |                |              |
   |S====+=======S| <- Disc         | Upper space  |
   |##############|                |              |
   |##############|                |     tab      |
   |##############|                |S====+=======S| <- Disc
   |##############|                |##############|
   +--------------+                +--------------+
      Fixed bottom                    Fixed bottom
```

### Parts

Use the Option A jar and closure.
Add an SS304 disc and one removable silicone perimeter lip.
Add a smooth lifting tab.
Provide a separate washable lifting hook if necessary.
Avoid a blind hole in the food-contact face.

### Operation

1. Remove the lid.
2. Lift the disc.
3. Remove the required spice.
4. Place the disc above the remaining spice.
5. Tilt the disc to let air escape.
6. Seat the lip without compressing the spice.
7. Install the lid.

The disc follows the spice level.
The outer lid remains the main storage closure.
The upper space remains inside the sealed jar.
The disc does not remove air between spice particles.

### Cleaning and limits

Remove the lip from the disc.
Wash the disc, lip, lifting tool, jar, lid, and gasket separately.

The tab must fit below the lid at maximum fill.
The disc must pass through the jar opening.
Check retrieval at the lowest spice level.
Check residue on both disc faces.
There is no bottom drive or retraction space.

**Recommended use:** First headspace and user-handling experiment.

## 4. Option C: Descending top piston with a smooth stem

### Section

```text
          REMOVABLE GLASS DOME
       +-----------------------+
       |       Handle          |
       |         |             |
       |      Smooth stem      | <- Stem storage space
       |         |             |
       +------ gasket ---------+
       | [Removable bridge]    |
       |      [Stem lock]      | <- Lock is not an air seal
       |         |             |
       |   Upper air space     |
       |         |             |
       |S========+===========S| <- Top piston
       |#######################|
       |#######################|
       +-----------------------+
            Fixed bottom
```

### Parts and motion

Use a standard closed-bottom jar.
Fit an SS304 piston with a removable silicone lip.
Attach a smooth stem to the upper piston face.
Support the stem with a removable bridge at the jar rim.
Use a releasable stem lock to hold the selected position.

The piston moves down as spice is removed.
The stem slides through the bridge.
The dome encloses the handle and the exposed stem.
The dome seals to the jar rim, outside the bridge.
Do not make a sliding stem penetration through the glass.

### Operation

Remove the dome and release the bridge before positioning the piston.
Use the stem to tilt the piston and release trapped air.
Seat the piston above the spice without compression.
Install the bridge and secure the stem.
Install the dome.

Remove the piston and bridge for spoon access.
Do not force the piston downward while its lip seals trapped air.

### Space and limits

The fixed jar bottom remains unchanged.
The dome must contain the maximum stem projection.
A 50 mm stroke can require about 50 mm of additional stem-storage height.
Handle and bridge dimensions require more space.
This is not a low-profile lid.

Use a tall transparent test cover for the first geometry experiment.
A production glass dome needs supplier review.
The bridge must not obstruct the lid gasket.
Check the stem joint, lock, and bridge for retained spice.

**Recommended use:** A retained top piston when a tall cover is acceptable.

## 5. Common arrangement for bottom-driven floors

Options D, E, F, and G use this separation.

```text
        REMOVABLE GLASS LID
       +--------------------+
       |  Headspace         |
       |####################|
       |####################|  Smooth SS304 bore
       |S==================S|  Food barrier and rising floor
       |                    |
       |     DRY REGION     |
       |   Drive and guides |
       |                    |
       +--- V -------- V ---+
            Base support
```

The smooth bore covers the entire seal travel.
Do not put slots or threads in that sealing surface.
Provide a controlled air path below the floor.
Protect the air path from dust and washing water.
Do not describe the dry region as hermetically sealed.

Use a removable drive module.
Provide access to the underside before releasing the floor.
Remove the floor through the top after disconnecting the drive.
Detailed release geometry remains necessary.
Do not expose a drive-release fastener on the spice face.

Provide a positive stop at each end of travel.
Provide guides independently of the perimeter seal.
Evaluate a clutch after normal torque and damage loads are known.
A clutch does not replace stops or moving-part retention.

## 6. Option D: Dry central lead screw

### Sections

```text
       LOW                             HIGH

   +------------------+            +------------------+
   |     Glass lid    |            |     Glass lid    |
   |##################|            |##################|
   |##################|            |S================S| <- Floor
   |##################|            | G |        | G   |
   |S================S| <- Floor   |   | Support|     |
   | G |        | G   |            |   | tube   |     |
   |   | Support|     |            |   +---N----+     |
   |   | tube   |     |            |       /          |
   |   |   /    |     |            |       /          |
   |   |   /    |     |            |       /          |
   |   +---N----+     | <- Nut     |       /          |
   +-------B----------+            +-------B----------+
       Bottom collar                   Bottom collar
```

### Parts and load path

Retain a rotating central screw in the base with axial bearings.
Attach its moving nut to a long support tube.
Close the support tube with the stainless-steel food floor.
The tube encloses the screw above the nut.

Fit separate guide rods and carriage bushes in the dry region.
The guides prevent floor rotation.
The screw converts rotation into movement.
The guides resist lateral load.
The base bearings transfer axial load to the housing.

```text
Hand -> collar -> optional clutch -> screw
Screw -> nut -> support tube -> floor
```

### Space and motion

Keep the screw tip below the floor at LOW.
The support tube needs enough length to cover the screw tip.
Provide about 50 mm of nut movement plus engagement and stop allowances.

Example: 50 mm stroke and 10 mm engagement need about 60 mm of active screw length.
The floor-to-nut offset and base add further height.
A dry region of approximately 60 mm or more is therefore a starting estimate.
This is not an 8 to 12 mm base.

Choose lead from torque results.
Keep the screw fixed axially while it rotates.
The nut, tube, and floor move together.

### Cleaning and limits

Disconnect the support tube from the dry side.
Remove the food floor and seal.
Keep screw wear and lubricant below the food barrier.
Check tube guidance, nut retention, and back-driving.

**Recommended use:** Simple motion demonstrator when a tall lower enclosure is acceptable.

## 7. Option E: Single-stage peripheral screw

### Sections

```text
       LOW                             HIGH

   +------------------+            +------------------+
   |     Glass lid    |            |     Glass lid    |
   |##################|            |##################|
   |##################|            |S================S| <- Floor
   |##################|            | G|            |G |
   |S================S| <- Floor   |  | Long skirt |  |
   | G|            |G |            |  |            |  |
   |  | Long skirt |  |            |  |            |  |
   |  |            |  |            | [N]          [N] | <- Nut
   | [N]          [N] | <- Nut     |                  |
   |  |            |  |            |  Vacated space   |
   +--B------------B--+            +--B------------B--+
       Bottom collar                   Bottom collar
```

The two nut sections represent one annular rotating nut.
The skirt has an external thread.
Thread details are omitted from the section.

### Parts and load path

Attach a long dry-side skirt to the food floor.
Fit a threaded polymer sleeve to the skirt.
Keep its attachment below the food face.
Engage the sleeve with an annular rotating nut.
Retain the nut axially in the base.

Use separate guides inside the hollow skirt.
Keep the floor and skirt non-rotating.
Provide bearing lands to control rocking.
Do not use the thread or silicone lip as the only guide.

```text
Hand -> collar -> optional clutch -> annular nut
Nut -> threaded skirt -> floor
```

### Space and motion

The nut rotates at a fixed height.
The skirt translates through the nut.
Its lower end rises as the floor rises.

For a 50 mm stroke, start with approximately 60 mm of working skirt.
This assumes 10 mm engagement for geometry only.
Calculate safe engagement separately.
Fit the entire retracted skirt inside the lower enclosure.
Add space for bearings, stops, and release parts.

A 10 mm lead gives five turns for 50 mm travel.
A single-start thread can provide this lead.
Do not add multiple starts without a specific thread-design reason.

### Cleaning and limits

Remove the lower drive module before releasing the floor.
Keep the perimeter lip removable.
Inspect the skirt attachment and underside for escaped powder.

This arrangement keeps the central region available for guides.
It does not provide a shallow base.
Its diameter includes the skirt, nut, bearings, and housing.

**Recommended use:** First bottom-collar drive study.

## 8. Option F: Two-stage peripheral screw

### Sections

```text
       RETRACTED                      EXTENDED

   +------------------+            +------------------+
   |##################|            |##################|
   |##################|            |S================S| <- Floor
   |S================S| <- Floor   |    | Stage B |    |
   |  | | Stage B | | |            |    |         |    |
   |  | +---------+ | |            |  [Overlap B]     |
   |  |   Stage A   | |            |  |  Stage A  |   |
   |  +-------------+ |            |  |           |   |
   | [Fixed housing]  |            | [Overlap A]      |
   +------------------+            | [Fixed housing]  |
                                   +------------------+
```

This is a packaging section.
Drive rings, guide parts, and bearings are not shown.
They require additional space.

### Defined bench arrangement

Use a fixed housing and two hollow threaded stages.
Stage A turns relative to the fixed housing.
Stage B turns relative to Stage A.
Support a non-rotating food floor on Stage B through an axial bearing.
Use a separate telescoping guide to prevent floor rotation.

Use two independently operated drive rings for the bench experiment.
Use a positive lock to hold Stage A while Stage B moves.
Expose the drive rings only in the dry bench region.
Use a removable guard during operation.

This arrangement avoids an undefined automatic stage sequence.
It does not provide a finished single-collar product.
A single collar needs a separate stage selector or coupled drive.
Its routing and space remain open.

### Operation

1. Release the Stage A lock.
2. Hold Stage B fixed relative to Stage A.
3. Turn Stage A to its extension stop.
4. Lock Stage A.
5. Turn Stage B to raise the floor further.

Retract Stage B before retracting Stage A.
Choose thread hands and turn directions from the motion drawing.
Do not assume both rings use the same direction.

### Space and limits

For geometry, consider two 35 mm stages with 10 mm overlap each.
Each stage then permits up to 25 mm movement before stop allowances.
The total theoretical movement is 50 mm.
Stops and other features reduce that movement.
Longer stages are necessary if they consume the movement allowance.

The collapsed drive cannot be treated as a 10 mm base.
Its height includes the nested stages, bearings, guides, and locks.
Its diameter includes nested wall thicknesses and thread clearances.
Check accumulated tilt at full extension.

Keep the non-rotating floor isolated from rotating stages.
Make the floor-bearing connection removable from the dry side.

**Recommended use:** Packaging experiment only if one-stage height is unacceptable.

## 9. Option G: Three-point helical cam

### Sections

```text
       LOW                             HIGH

   +------------------+            +------------------+
   |##################|            |##################|
   |##################|            |S================S| <- Floor
   |##################|            | |              | |
   |S================S| <- Floor   | | Lift posts   | |
   | |              | |            | G---o------o---G | <- Carriage
   | | Lift posts   | |            |   /        /    |
   | |              | |            |  / Long   /     |
   | G---o------o---G | <- Carriage | / barrel /      |
   |   /        /    |            |/        /       |
   +-------B----------+            +-------B----------+
       Bottom collar                   Bottom collar
```

Two lift posts and followers appear in the section.
The third support is outside the section plane.
Three supports are spaced at 120 degrees.

### Parts and load path

Use a long rotating barrel below the food floor.
Provide three coordinated helical tracks.
Fit retained followers to a non-rotating carriage.
Connect the carriage to the floor with three lift posts.

Guide the carriage with separate fixed guides.
Retain the barrel axially with base bearings.
Phase the tracks so all followers remain at the same height.
Check track separation throughout all revolutions.

```text
Hand -> collar -> optional clutch -> rotating barrel
Tracks -> followers -> carriage -> lift posts -> floor
```

### Space and motion

The barrel stays at a fixed height.
The followers and carriage move along the tracks.
The lift posts keep the floor above the barrel.

A 50 mm stroke needs at least 50 mm of track rise.
Add follower size, end retention, and stop allowances.
The lift-post length keeps the barrel below the floor at LOW.
The resulting lower enclosure is tall.
This design does not hide long tracks inside a short base ring.

A 10 mm rise per revolution gives five turns for 50 mm movement.
Measure torque before selecting that rise.
Verify follower capture during both movement directions.

### Cleaning and limits

Remove the dry drive module before releasing the floor connections.
Check all three follower interfaces for wear.
Do not use three independent cam adjustments.
Do not rely on the perimeter seal to prevent carriage rotation.

**Recommended use:** Cam comparison after the simpler screw arrangement.

## 10. Selection and next drawings

| Option | Retraction space | Bottom collar | Main development issue |
|---|---|---|---|
| A: Conventional jar | None | No | No continuous adjustment |
| B: Follower disc | Upper jar space | No | Retrieval and powder at the lip |
| C: Top piston | Tall lid for the stem | No | Bridge, stem storage, and air release |
| D: Central screw | Long lower enclosure | Yes | Screw coverage and carriage guidance |
| E: Peripheral screw | Long lower enclosure | Yes | Skirt retraction and annular nut support |
| F: Two-stage screw | Shorter than one long stage, subject to parts | Not yet a single collar | Stage drive, locking, and guide clearance |
| G: Cam | Long lower enclosure | Yes | Track geometry and retained followers |

Build Options A and B first.
Use their results to decide whether a drive adds sufficient value.
Study Option E first if bottom operation is required.

For each driven option, produce a dimensioned LOW and HIGH section.
Show retained overlap, end stops, bearings, guides, and air paths.
Show food-contact part removal.
Calculate capacity and external dimensions from those sections.
Do not use these text drawings as production geometry.
