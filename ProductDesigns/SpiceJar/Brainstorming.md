# Variable-Volume Spice Jar: Engineering Findings

**Status:** Concept review. No production design is approved.
**Related document:** [Product design brief](ProductDesignBrief.md).
**Concept drawings:** [Text concept designs](ConceptDesigns.md).

## 1. Purpose and terms

Reduce the empty space above stored spice.
Keep operation and cleaning simple.
Use no disposable bag, motor, battery, or electronic control.

| Term | Meaning |
|---|---|
| Headspace | Empty space above the spice |
| Follower disc | Removable disc placed above the spice |
| Piston | Moving part that changes the spice chamber volume |
| Rising floor | Piston below the spice |
| Skirt | Cylindrical extension below a rising floor |
| Stroke | Total linear movement |
| Lead | Linear movement for one screw revolution |
| Pitch | Axial distance between adjacent thread forms |
| Dry region | Mechanism space separated from the spice; not necessarily airtight |
| Back-driving | Load-induced reverse movement of the drive |
| Envelope volume | Volume of the specified external package boundary |

Use these technical names consistently.
Use short sentences and direct instructions.
Full ASD-STE100 conformity requires a review against the applicable approved vocabulary.

## 2. Main findings

Reduced headspace can reduce the quantity of free air above the spice.
It does not establish longer shelf life.
Compare spice quality with a conventional gasketed jar before developing a complex drive.

The stainless-steel body is a useful option.
It removes the need for a precision glass sliding surface.
It does not remove the space required for drive retraction.

A removable follower disc is the recommended first experiment.
A single-stage peripheral screw is the preferred drive study if a bottom collar is essential.
Do not select a multistage drive or cam until the complete geometry is defined.

## 3. Preservation limits

Reduced headspace does not remove air between spice particles.
It does not remove moisture already in the spice.
Opening, spooning, light, heat, and seal leakage can affect spice quality.
Storage near a stove can increase exposure to heat and moisture.

Powder exclusion and gas sealing are different functions.
A clean mechanism does not prove low oxygen or water-vapour ingress.
Silicone can transmit gases and water vapour.
Material grade alone does not establish acceptable storage performance.

Use equal spice quantities and controlled exposure in comparison tests.
Measure aroma retention, caking, and moisture uptake.
Define the required improvement before claiming a preservation benefit.

## 4. Requirements and choices

Keep food-contact surfaces primarily glass, stainless steel, and silicone.
Keep threads, lubricant, and wear particles out of the spice chamber.
Make food-contact parts removable and washable.

A glass lid with a stainless-steel body is an option.
It permits top-view identification only.
Use labels if top visibility is insufficient.
An opaque body protects spice from light.

The bottom collar is a design preference, not a confirmed requirement.
Whole seeds and flakes are outside the first experiment.
Their compatibility needs separate evidence.

## 5. Capacity correction

Do not use "100 mL jar" for both capacity and external volume.
State usable capacity, external dimensions, and adjustment range separately.
Measure usable capacity below the fill line.

For an ideal cylinder:

```text
V_mL = pi * diameter_mm^2 * height_mm / 4000
Utilization_percent = 100 * usable_capacity_mL / envelope_volume_mL
```

| Geometry | Calculated volume |
|---|---:|
| 45 mm internal diameter; 55 mm usable height | 87.5 mL |
| 50 mm external diameter; 65 mm body height | 127.6 mL |
| 55 mm external diameter; 72 mm body height | 171.1 mL |

The external examples exclude the lid.
They assume cylindrical bodies without projections.
The corresponding utilization is approximately 69% and 51%, not 85% to 90%.

These calculations do not establish mechanism fit.
Calculate final capacity from CAD geometry and measured fill volume.
Include walls, seals, guides, stops, and drive parts.
Radial placement can reduce axial stacking but increases diameter or reduces the bore.

## 6. Mechanism options

| Option | Benefit | Limitation |
|---|---|---|
| Conventional jar in smaller sizes | Few parts; easy cleaning | No continuous volume adjustment |
| Removable follower disc | No bottom drive; standard closed-bottom jar | Remove before spooning; provide retrieval and air release |
| Descending top piston | Standard jar bottom; removable drive assembly | Stem or lid needs space; air release remains necessary |
| Dry central lead screw | Direct motion relationship | Screw and guides need space below the floor |
| Single-stage peripheral screw | Broad support; central region remains available | Long skirt needs retraction space |
| Multistage peripheral screw | Greater stroke from a shorter collapsed assembly | More stages, clearance, radial space, and motion control |
| Three-point cam | Three support points; common rotary drive | Long tracks, guidance, retention, and packaging remain unresolved |

A central screw below the food barrier is not inherently unsuitable.
Its exclusion needs a packaging or cleaning reason.
Do not reject it only because it is a screw.

### 6.1 Follower disc

Place an SS304 disc with a removable silicone lip above the spice.
Keep the outer lid as the main storage closure.
Provide a lifting feature and, if necessary, a removable lifting hook.

Tilt the disc or release the lip during placement to let air escape.
Do not force a sealed disc onto the spice.
A single removable lip is the starting option.
Add a second lip or valve only if a measured failure requires it.

### 6.2 Three-point cam

Use one drive with three coordinated tracks, if this option is retained.
Three support points do not provide an anti-rotation guide.
Seal friction is not a reliable guide.

Tracks that produce 50 to 60 mm stroke need corresponding axial travel geometry.
An ordinary track ring inside an 8 to 12 mm base cannot provide that stroke alone.
Show the long carrier or extending parts in both end positions.

A steep cam can increase torque and back-driving.
Choose lead from measured friction and acceptable user effort.
Do not assume fewer turns than a screw with the same lead.

### 6.3 Peripheral screw and inverted cup

A closed upper face with a downward skirt is an inverted cup-shaped piston.
It is not an open cup that contains the spice.

For a fixed base nut, use this preliminary relationship:

```text
Working skirt length >= stroke + retained thread engagement
```

A 50 mm stroke with 10 mm engagement needs about 60 mm of working skirt.
This is an example, not a specified engagement length.
Add space for stops and other features.
Determine engagement from load, material, guidance, and wear.

At LOW, the skirt must fit below the floor.
It cannot disappear into the annular gap.
Provide a lower enclosure, permitted external extension, or additional nested stages.

For nested stages, a preliminary stroke estimate is:

```text
Stroke <= sum(stage length - retained overlap)
```

Stops and drive features reduce available stroke.
A short collapsed base with a long stroke can require many stages.
Clearance accumulates between stages and can increase floor rocking.
Provide separate bearing surfaces and sufficient overlap.

Define which member rotates and which member is restrained.
Define whether stages move together or in sequence.
Provide positive retention and reliable reverse movement.
Otherwise, the interface with the least resistance can move first.

Keep the seal bore smooth throughout its travel.
Keep threads and guide interruptions outside that bore.
A rotating drive sleeve needs bearing support and axial retention.
A non-rotating piston needs a separate guide.
Include these functions in the part count.

Allowing the floor to rotate can simplify the drive.
It can also increase seal abrasion and disturb the spice.
A non-rotating floor is preferred for the drive study.

### 6.4 Thread selection

```text
Lead = number of starts * pitch
Turns = stroke / lead
```

A 10 mm lead gives five turns for 50 mm stroke.
For three starts, the pitch is approximately 3.33 mm.
A single-start thread can also have a 10 mm lead.

Select lead before the number of starts.
Multiple starts do not guarantee lower friction or equal load sharing.
Self-locking depends on lead angle and friction.
Do not assume a thread prevents back-driving.

## 7. Cam and screw comparison

The ratings depend on complete geometry, not the mechanism name.

| Criterion | Three-point cam | Peripheral screw |
|---|---|---|
| Floor stability | Needs guides and controlled follower clearance | Needs bearing overlap and controlled sleeve clearance |
| Part count | Includes followers and their retention | May be lower for one stage; increases with stages |
| Central space | Can remain open | Can remain open with hollow sleeves |
| Collapsed height | Must accommodate track and carrier | Must accommodate retracted sleeves and engagement |
| Manufacture | Long internal tracks can complicate tooling | Nested internal and external threads can complicate tooling |
| Tilt resistance | Depends on guidance and floor stiffness | Depends on guidance and accumulated stage clearance |
| Cleaning separation | Depends on seals and joints | Depends on seals and joints |
| Clutch installation | Possible | Possible; torque can vary between stages |
| Long stroke | Needs a long carrier or extending structure | Needs retraction space or nested stages |
| Prototype work | Complete geometry remains necessary | One stage is simpler than controlled nested stages |
| Production suitability | Not established | Not established |

## 8. Materials, seals, and pressure

SS304 is the starting food-contact metal.
Use SS316L only if corrosion or cleaning conditions require it.
Specify surface finish and food-contact compliance for the intended market.

Deep drawing can produce a suitable chamber.
It does not guarantee a precision bore.
Control taper, ovality, springback, and surface finish.

An ordinary closed-bottom glass jar cannot directly accept a drive from below.
An open-bottom glass cylinder needs a retained, sealed base joint.
A follower disc or top piston avoids this connection.

Evaluate removable silicone profiles for powder exclusion, friction, and cleaning.
Test folding, tearing, abrasion, staining, aroma retention, and compression set.
A second lip adds friction and a space that can retain residue.
It is a secondary barrier, not a guarantee.

Choose dry-region polymers from load, wear, creep, temperature, and cleaning exposure.
Do not use lubricants or coatings that can migrate into the spice.

Move a piston with the lid open during initial use.
A closed lid can trap compressed air above the piston.
The changing space below the floor also needs an air path.
Control dust entry and cleaning-water entry through that path.
An exhaust valve adds leakage and cleaning risks.

## 9. Protection and cleaning

A slip clutch limits input torque, not directly the force on the spice.
Axial force also depends on drive efficiency and seal friction.
Measure normal torque and overload behaviour before setting clutch torque.
Test detent wear, creep, repeated slipping, and temperature effects.
Do not use clutch slipping as normal position control.
Use positive end stops and retain all moving parts.

Multi-turn drives need position information beyond a repeating collar mark.
A simple LOW or HIGH mark on the collar does not identify accumulated turns.

Provide access to both sides of each food-contact seal.
Avoid exposed fasteners, threads, grease, adhesive, sharp corners, and inaccessible cavities.
Demonstrate tool-free removal instead of assuming a service position makes it possible.
Do not claim dishwasher suitability without wash and water-entry evidence.

## 10. Corrections to earlier conclusions

| Earlier claim | Corrected finding |
|---|---|
| Cam architecture is feasible and selected | Travel geometry and packaging are unresolved |
| 85% to 90% utilization follows from the example dimensions | The examples give approximately 51% to 69% |
| A short base can contain the full helical lift | Additional axial structure is necessary |
| Peripheral threads remove the need for lower space | A rigid skirt still needs retraction space |
| Threads inherently prevent tilt | Separate guidance and adequate overlap are necessary |
| Dual lips keep all powder out | Powder exclusion needs evidence |
| A powder barrier proves an airtight chamber | Gas and moisture ingress need separate evidence |
| Stainless-steel forming guarantees better tolerances | Manufacturing capability needs supplier evidence |
| Clutch torque guarantees safe compression force | Axial force and failure loads must be measured |
| Lower part count and premium pricing are established | Full assembly counts and market evidence are absent |

Earlier cost ranges have no supplier evidence.
They are not retained as estimates.
The previous INR 300 factory-cost objective is an unapproved commercial goal.
Obtain quotations after geometry, processes, and quantities are defined.
Include tooling, assembly, inspection, rejects, packaging, freight, taxes, and warranty.
Neither single-jar pricing nor multi-jar set economics are established.

## 11. Development decision

First compare a follower disc with a conventional gasketed jar.
Use dry ground spices.
Record handling, cleaning, sealing, and spice-quality results.

If the benefit is useful, study a single-stage peripheral screw.
Show dimensioned LOW and HIGH sections before building the drive.
Accept a taller base if this removes unnecessary stages.
Keep the cam as an alternative, not an approved fallback.

Use the related brief for prototype requirements, proposed test limits, and decision gates.
